#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

const int BUFFER_SIZE = 1024;

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct connection {
  struct ibv_qp *qp;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;

  char *recv_region;
  char *send_region;
};

static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static void on_completion(struct ibv_wc *wc);
static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
#if _USE_IPV6
  struct sockaddr_in6 addr;     //ipv6地址
#else
  struct sockaddr_in addr;      //ipv4地址
#endif
  struct rdma_cm_event *event = NULL;   //rdma_cm_event事件——>event 之后会通过rdma_get_cm_event获取从evnet channel上面获取事件并赋值

  struct rdma_cm_id *listener = NULL;   //类似建立了一个listen的socket
  struct rdma_event_channel *ec = NULL; //一个event的channel
  uint16_t port = 0;                    //类似socket编程绑定的port端口

  memset(&addr, 0, sizeof(addr));       //对IP地址初始化
#if _USE_IPV6
  addr.sin6_family = AF_INET6;
#else
  addr.sin_family = AF_INET;
#endif

  TEST_Z(ec = rdma_create_event_channel());         //创建一个event_channel——event channel是RDMA设备在操作完成后，或者有连接请求等事件发生时，用来通知应用程序的通道。——其内部就是一个file descriptor, 因此可以进行poll等操作。
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));    //这一步创建一个rdma_cm_id, 概念上等价与socket编程时的listen socket。
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));  //和socket编程一样，也要先绑定一个本地的地址和端口，以进行listen操作。
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */   //开始侦听客户端的连接请求

  port = ntohs(rdma_get_src_port(listener));    //rdma_get_src_port retrieves the local port number for an rdma_cm_id (id) which has been bound to a local address. If the id is not bound to a port, the routine will return 0 获取listener的端口

  printf("listening on port %d.\n", port);

  while (rdma_get_cm_event(ec, &event) == 0) {  //这个调用就是作用在第一步创建的event channel上面，要从event channel中获取一个事件。这是个阻塞调用，只有有事件时才会返回。在一切正常的情况下，函数返回时会得到一个 RDMA_CM_EVENT_CONNECT_REQUEST事件，也就是说，有客户端发起连接了。 在事件的参数里面，会有一个新的rdma_cm_id传入。这点和socket是不同的，socket只有在accept后才有新的socket fd创建。
    
    struct rdma_cm_event event_copy;        //建立一个temp event_copy

    memcpy(&event_copy, event, sizeof(*event));     //将获取到的event拷贝下来


    rdma_ack_cm_event(event);           //对于每个从event channel得到的事件，都要调用ack函数，否则会产生内存泄漏。这一步的ack是对应rdma_get_cm_event的get。每一次get调用，都要有对应的ack调用。

    if (on_event(&event_copy))          //这里应该是调用了on_event对当前获得的event进行一次判断，如果event满足某种条件就放弃从channel里面继续获取事件。
      break;
  }

  rdma_destroy_id(listener);            //释放用于监听的rdma_cm_id
  rdma_destroy_event_channel(ec);       //释放evnet channel

  return 0;
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

void build_context(struct ibv_context *verbs)
{
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx)); //创建一个protection domain。protection domain可以看作是一个内存保护单位，在内存区域和队列直接建立一个关联关系，防止未授权的访问。
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));    //和之前创建的event channel类似，这也是一个event channel，但只用来报告完成队列里面的事件。当完成队列里有新的任务完成时，就通过这个channel向应用程序报告
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */    //创建完成队列，创建时就指定使用ibv_alloc_pd的channel。

  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

void * poll_cq(void *ctx)
{
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}

void post_receives(struct connection *conn)
{
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->recv_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
  conn->send_region = malloc(BUFFER_SIZE);
  conn->recv_region = malloc(BUFFER_SIZE);

  TEST_Z(conn->send_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->send_region,
    BUFFER_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_mr = ibv_reg_mr(
    s_ctx->pd,
    conn->recv_region,
    BUFFER_SIZE,
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}

void on_completion(struct ibv_wc *wc)
{
  if (wc->status != IBV_WC_SUCCESS)
    die("on_completion: status is not IBV_WC_SUCCESS.");

  if (wc->opcode & IBV_WC_RECV) {
    struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

    printf("received message: %s\n", conn->recv_region);

  } else if (wc->opcode == IBV_WC_SEND) {
    printf("send completed successfully.\n");
  }
}

int on_connect_request(struct rdma_cm_id *id)
{
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;
  struct connection *conn;

  printf("received connection request.\n");

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));        //创建一个queue pair, 一个queue pair包括一个发送queue和一个接收queue. 指定使用前面创建的cq作为完成队列。该qp创建时就指定关联到ibv_alloc_pd创建的pd上。

  id->context = conn = (struct connection *)malloc(sizeof(struct connection));
  conn->qp = id->qp;

  register_memory(conn);        //注册内存区域，ibv_reg_mr，注册内存区域。RDMA使用的内存，必须事先进行注册。这个是可以理解的，DMA的内存在边界对齐，能否被swap等方面，都有要求。

  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));
  TEST_NZ(rdma_accept(id, &cm_params));         //至此，做好了全部的准备工作，可以调用accept接受客户端的这个请求了。

  return 0;
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;
  struct ibv_send_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  snprintf(conn->send_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());

  printf("connected. posting send...\n");

  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->send_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));

  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  struct connection *conn = (struct connection *)id->context;

  printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->send_mr);
  ibv_dereg_mr(conn->recv_mr);

  free(conn->send_region);
  free(conn->recv_region);

  free(conn);

  rdma_destroy_id(id);

  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)            //当前事件意味客户端发起链接了
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)           //当前事件意味客户端已经建立建立起来了
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)          //表示客户端断开了链接，server端要进行对应的清理。此时可以调用rdma_ack_cm_event释放事件资源。然后依次调用下面的函数，释放连接资源，内存资源，队列资源。
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

