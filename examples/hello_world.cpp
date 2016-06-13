#include <string>
#include <fstream>
#include <iostream>

#include "caf/all.hpp"

using std::cout;
using std::endl;
using std::string;


namespace {

using namespace caf;

class config : public actor_system_config {
public:
  size_t num_sources = 20;
  size_t num_msgs = 1000;

  config() {
    opt_group{custom_options_, "global"}
    .add(num_sources, "num-sources,s", "nr. of sources")
    .add(num_msgs, "num-messages,n", "nr. of messages");
  }
};

template <class F>
using signature_t = typename detail::get_callable_trait<F>::fun_sig;

#if 0

struct source_state {
  int x = 0;
};

behavior source(stateful_actor<source_state>* self, actor target,
                size_t num_msgs) {
  auto sh = self->new_stream(target, [=]() -> result<int> {
    auto x = self->state.x++;
    if (x == static_cast<int>(num_msgs)) {
      self->quit();
      return skip();
    }
    return x;
  });
  return {[=] {
    // dummy
  }};
}

struct sink_state {
  static const char* name;
  size_t received_messages = 0;
  ~sink_state() {
    printf("received messages: %d\n", (int)received_messages);
  }
};

const char* sink_state::name = "foobar-sink";

behavior sink(stateful_actor<sink_state>* self) {
  return {
    [=](int) {
      if (++self->state.received_messages % 10000 == 0) {
        printf("~~~ received messages: %d\n",
               (int) self->state.received_messages);
      }
    }
  };
}

behavior forward_stage(event_based_actor*) {
  return {
    [=](int x) {
      return x;
    }
  };
}

template <class T>
std::ostream& operator<<(std::ostream& x, const std::vector<T>& ys) {
  return x << deep_to_string(ys);
}

template <class T>
std::ostream& operator<<(std::ostream& x, const optional<T>& ys) {
  return x << to_string(ys);
}

#define CAF_STR(s) #s

#define CAF_XSTR(s) CAF_STR(s)

#define SHOW(x) cout << CAF_XSTR(x) << typeid(x).name() << endl

/// A basic stream is a stream that accepts a consumer
template <class T>
class basic_stream {
public:
  basic_stream(std::function<void (T)> consumer) : consumer_(std::move(consumer)) {
    // nop
  }

  inline size_t open_credit() const {
    return open_credit_;
  }

  // called whenever new credit is available
  /// Returns `false` if the stream reached its end.
  virtual bool consume(size_t up_to) = 0;

private:
  size_t open_credit_;
  std::function<void (T)> consumer_;
};

namespace streaming {

template <class T>
class source : public ref_counted {
public:
  virtual optional<T> pull() = 0;
};

template <class T>
using source_ptr = intrusive_ptr<source<T>>;

template <class T>
class sink {
public:
  virtual ~sink(){
    // nop
  }

  virtual void push(T) = 0;
};

template <class T>
class origin : public sink<T>, public source<T> {};

template <class T>
using origin_ptr = intrusive_ptr<origin<T>>;

template <class In, class Out>
class stage : public source<Out> {
public:
  using prev_stage = source_ptr<In>;

  stage(prev_stage prev) : prev_(prev) {
    // nop
  }

  source<In>& prev() const {
    return *prev_;
  }

private:
  prev_stage prev_;
};

template <class In, class Out>
using stage_ptr = intrusive_ptr<stage<In, Out>>;

// origin -> stage_0 -> ... -> stage_n-1 -> stage_n

template <class F>
using return_type_t = typename detail::get_callable_trait<F>::result_type;

template <class F>
using argument_type_t = typename detail::tl_head<typename detail::get_callable_trait<F>::arg_types>::type;

template <class F>
class mapping_stage : public stage<argument_type_t<F>, return_type_t<F>> {
public:
  using super = stage<argument_type_t<F>, return_type_t<F>>;

  using prev_stage = typename super::prev_state;

  mapping_stage(F fun, prev_stage* prev) : super(prev), fun_(std::move(fun)) {
    // nop
  }

  optional<return_type_t<F>> pull() override {
    auto x = this->prev()->pull();
    if (!x)
      return none;
    return fun_(std::move(*x));
  }

private:
  F fun_;
};

template <class Out>
class consumer : public ref_counted {
public:
  virtual void consume(std::vector<Out>& buf) = 0;

  virtual void shutdown() {
    // nop
  }
};

template <class T>
using consumer_ptr = intrusive_ptr<consumer<T>>;

/// A channel is a DAG with a single root and a single end.
template <class In, class Out>
class pipeline : public ref_counted {
public:
  pipeline(origin_ptr<In> root, source_ptr<Out> endpoint, consumer_ptr<Out> f)
      : closed_(false),
        open_credit_(0),
        v0_(std::move(root)),
        vn_(std::move(endpoint)),
        f_(std::move(f)) {
    // nop
  }

  /// Pushes a single element to the channel.
  template <class Iterator, class Sentinel>
  void push(Iterator i, Sentinel e) {
    CAF_ASSERT(!closed_);
    if (i == e)
      return;
    do {
      v0_->push(*i);
    } while (++i != e);
    if (open_credit_ > 0)
      trigger_consumer();
  }

  void push(std::initializer_list<In> xs) {
    push(xs.begin(), xs.end());
  }

  /// Pushes a single element to the channel.
  void push(In x) {
    CAF_ASSERT(!closed_);
    v0_->push(std::move(x));
    if (open_credit_ > 0)
      trigger_consumer();
  }

  /// Closes the write end of the channel.
  void close() {
    closed_ = true;
  }

  /// Allows the consumer to eat up `x` more items.
  void grant_credit(size_t x) {
    CAF_ASSERT(x > 0);
    open_credit_ += x;
    trigger_consumer();
  }

  size_t open_credit() {
    return open_credit_;
  }

private:
  void trigger_consumer() {
    size_t i = 0;
    for (; i < open_credit_; ++i) {
      auto x = vn_->pull();
      if (x)
        buf_.emplace_back(std::move(*x));
      else
        break;
    }
    if (i > 0) {
      f_->consume(buf_);
      buf_.clear();
    }
    // we can shutdown the consumer if the channel is closed and we could not
    // produce a sufficient number of items, because no new elements will appear
    if (i < open_credit_ && closed_)
      f_->shutdown();
    open_credit_ -= i;
  }

  bool closed_;
  size_t open_credit_;

  origin_ptr<In> v0_;
  source_ptr<Out> vn_;
  consumer_ptr<Out> f_;
  std::vector<Out> buf_;
};

emplate <class T, class S = signature_t<T>>
struct arg_type;

template <class T, class B, class A>
struct arg_type<T, B(A)> {
  using type = A;
};

template <class T>
using arg_type_t = typename arg_type<T>::type;

template <class T, class S = signature_t<T>>
struct result_type;

template <class T, class B, class... As>
struct result_type<T, B(As...)> {
  using type = B;
};

template <class T>
using result_type_t = typename result_type<T>::type;

template <class F, class S = signature_t<F>>
struct is_map_fun : std::false_type {};

template <class F, class R, class T>
struct is_map_fun<F, R (T)> : std::true_type {};

template <class F, class T>
struct is_map_fun<F, bool (T)> : std::false_type {};

template <class F, class S = signature_t<F>>
struct is_filter_fun : std::false_type {};

template <class F, class T>
struct is_filter_fun<F, bool (T)> : std::true_type {};

template <class F, class S = signature_t<F>>
struct is_reduce_fun : std::false_type {};

template <class F, class B, class A>
struct is_reduce_fun<F, void (B&, A)> : std::true_type {};

template <class F, class S = signature_t<F>>
struct first_arg_type;

template <class F, class R, class T, class... Ts>
struct first_arg_type<F, R (T, Ts...)> {
  using type = T;
};

template <class F>
using first_arg_type_t = typename first_arg_type<F>::type;

template <class F, class S = signature_t<F>>
struct second_arg_type;

template <class F, class R, class _, class T, class... Ts>
struct second_arg_type<F, R (_, T, Ts...)> {
  using type = T;
};

template <class F>
using second_arg_type_t = typename second_arg_type<F>::type;

template <class In, class Out, class... Fs>
struct stream_builder {
  std::tuple<Fs...> fs;
};

template <class T, class Out>
struct eval_helper {
public:
  eval_helper(T& x) : x_(x) {
    // nop
  }

  template <class... Fs>
  optional<Out> operator()(Fs&... fs) {
    return apply(x_, fs...);
  }

private:
  optional<Out> apply(Out& x) {
    return std::move(x);
  }

  template <class X, class F, class... Fs>
  typename std::enable_if<is_filter_fun<F>::value, optional<Out>>::type
  apply(X& x, F& f, Fs&... fs) {
    if (!f(x))
      return none;
    return apply(x, fs...);
  }

  template <class X, class F, class... Fs>
  typename std::enable_if<is_map_fun<F>::value, optional<Out>>::type
  apply(X& x, F& f, Fs&... fs) {
    auto y = f(std::move(x));
    return apply(y, fs...);
  }

  T& x_;
};

template <class In, class Out, class... Fs>
optional<Out> eval(stream_builder<In, Out, Fs...> sb, In x) {
  using namespace detail;
  eval_helper<In, Out> h{x};
  return apply_args(h, typename il_range<0, sizeof...(Fs)>::type{}, sb.fs);
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_filter_fun<F>::value>::type>
stream_builder<In, Out, Ts..., F>
operator|(stream_builder<In, Out, Ts...> x, F f) {
  static_assert(std::is_same<Out, arg_type_t<F>>::value,
                "invalid filter signature");
  return {std::tuple_cat(std::move(x.fs), std::make_tuple(f))};
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_map_fun<F>::value>::type>
stream_builder<In, result_type_t<F>, Ts..., F>
operator|(stream_builder<In, Out, Ts...> x, F f) {
  static_assert(std::is_same<Out, arg_type_t<F>>::value,
                "invalid map signature");
  return {std::tuple_cat(std::move(x.fs), std::make_tuple(f))};
}

template <class In, class Out, class... Ts, class F,
          class E = typename std::enable_if<is_reduce_fun<F>::value>::type>
result<typename std::decay<first_arg_type_t<F>>::type>
operator>>(stream_builder<In, Out, Ts...>, F) {
  return delegated<typename std::decay<first_arg_type_t<F>>::type>{};
}

template <class In, class Out, class... Fs>
stage_ptr<In, Out> make_stage(source_ptr<In> prev,
                              stream_builder<In, Out, Fs...>&& sb) {
  class impl : public stage<In, Out> {
  public:
    using super = stage<In, Out>;
    using tuple_type = std::tuple<Fs...>;
    using prev_stage = source_ptr<In>;

    impl(prev_stage ptr, tuple_type&& fs)
        : super(std::move(ptr)),
          fs_(std::move(fs)) {
      // nop
    }

    optional<Out> pull() {
      using namespace detail;
      auto x = this->prev().pull();
      eval_helper<In, Out> h{x};
      return apply_args(h, typename il_range<0, sizeof...(Fs)>::type{}, fs_);
    }

  private:
    tuple_type fs_;
  };
  return make_counted<impl>(std::move(prev), std::move(sb.fs));
}

std::pair<std::string, int> flatten(std::map<std::string, int>) {
  return std::make_pair("", 0);
}

} // namespace streaming

class ipair_pipe : public streaming::origin<std::pair<int, int>> {
public:
  using value_type = std::pair<int, int>;

  optional<value_type> pull() {
    if (xs_.empty())
      return none;
    auto x = xs_.front();
    xs_.erase(xs_.begin());
    return x;
  }

  void push(value_type x) {
    xs_.emplace_back(x);
  }

private:
  std::vector<value_type> xs_;
};

class stage1 : public streaming::stage<std::pair<int, int>, int> {
public:
  using super = streaming::stage<std::pair<int, int>, int>;

  using super::super;

  optional<int> pull() {
    while (xs_.empty()) {
      auto new_x = prev().pull();
      if (new_x == none)
        return none;
      for (int i = 0; i < new_x->first; ++i)
        xs_.push_back(new_x->second);
    }
    auto x = xs_.front();
    xs_.erase(xs_.begin());
    return x;
  }

private:
  std::vector<int> xs_;
};

class test_consumer : public streaming::consumer<int> {
public:
  void consume(std::vector<int>& buf) override {
    cout << "test_consumer: " << deep_to_string(buf) << endl;
  }

  void shutdown() override {
    cout << "test_consumer::shutdown" << endl;
  }
};

template <class T>
struct stream {
public:
  template <class In, class... Fs>
  stream(streaming::stream_builder<In, T, Fs...>) {
    // nop
  }
};

template <class T>
struct is_stream : std::false_type {};

template <class T>
struct is_stream<stream<T>> : std::true_type {};

} // namespace <anonymous>

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<void>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<int>)
CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<std::string>)

namespace {

template <class T>
stream<T> to_stream(T) {
  return {};
}

template <class T>
struct strip_optional;

template <class T>
struct strip_optional<optional<T>> {
  using type = T;
};

template <class T>
using strip_optional_t = typename strip_optional<T>::type;

template <class T>
streaming::stream_builder<T, T> transform(stream<T>&&) {
  return {};
}

streaming::stream_builder<std::string, std::string>
file_source(const std::string&) {
  return {};
}

template <class T, class Destination>
struct stream_result {
  // t: trait representing the response type of the Destination for e
  using t = response_type<typename Destination::signatures, stream<T>>;
  static_assert(t::valid, "Destination does not accept streams of that type");
  // result type
  using type = typename t::type;
  static_assert(detail::tl_size<type>::value == 1,
                "Destination does not terminate the stream");
  static_assert(!is_stream<typename detail::tl_head<type>::type>::value,
                "Destination does not terminate the stream");
};

template <class Generator, class Destination>
using stream_result_t = typename stream_result<Generator, Destination>::type;

class event_based_actor_ : public event_based_actor {
public:
  event_based_actor_(actor_config& cfg) : event_based_actor(cfg) {
    // nop
  }

  template <class Dest, class Generator>
  response_handle<event_based_actor_, stream_result_t<strip_optional_t<streaming::result_type_t<Generator>>, Dest>, false>
  stream(Dest, Generator) {
    return {message_id::make(), this};
  }
};

// maps have signature A -> B
// filters have signature A -> bool
// terminal ops have signature A -> void

using foo1_actor = typed_actor<replies_to<stream<int>>::with<stream<std::string>>>;

foo1_actor::behavior_type foo1(foo1_actor::pointer) {
  return {
    // stream handshake
    [=](stream<int>& source) -> stream<std::string> {
      return transform(std::move(source))
        | [](int x) { return x * 2.0; }
        | [](double x) { return x > 5.; }
        | [](double x) { return std::to_string(x); }
        ;
    }
  };
}

behavior foo2(event_based_actor_* self) {
  return {
    [=](std::string& x) {
      aout(self) << x << endl;
    }
  };
}

using path = std::string;
using wc_pair = std::pair<std::string, int>;
using mapped_stream = stream<wc_pair>;
using result_map = std::map<std::string, int>;

using file_mapper = typed_actor<replies_to<path>::with<mapped_stream>>;
using reducer = typed_actor<replies_to<mapped_stream>::with<result_map>>;

reducer::behavior_type reducer_impl() {
  return {
    [](mapped_stream& x) -> result<result_map> {
      return transform(std::move(x))
        >> [](result_map& res, wc_pair x) {
             auto i = res.find(x.first);
             if (i == res.end())
               res.emplace(std::move(x.first), x.second);
             else
               i->second += x.second;
           };
    }
  };
}

file_mapper::behavior_type file_mapper_impl() {
  return {
    [](path& p) -> mapped_stream {
      return file_source(p)
        | [](std::string line) -> std::map<std::string, int> {
            std::map<std::string, int> result;
            std::vector<std::string> lines;
            split(lines, line, " ", token_compress_on);
            for (auto& x : lines)
              ++result[x];
            return result;
          }
        | streaming::flatten;
    }
  };
}

void test(actor_system& sys) {
  auto f = sys.spawn(file_mapper_impl);
  auto g = sys.spawn(reducer_impl);
  auto pipeline = g * f;
  scoped_actor self{sys};
  self->request(pipeline, infinite, "words.txt").receive(
    [&](result_map& result) {
      aout(self) << "result = " << deep_to_string(result) << endl;
    },
    [&](error& err) {
      aout(self) << "error: " << sys.render(err) << endl;
    }
  );
}

using foo3_actor = typed_actor<replies_to<stream<int>>::with<void>>;

foo3_actor::behavior_type foo3(foo3_actor::pointer self) {
  return {
    // stream handshake
    [=](stream<int>& source) {
      transform(std::move(source))
        | [](int x) { return x * 2.0; }
        | [](double x) { return x > 5.; }
        | [](double x) { return std::to_string(x); }
        | [=](std::string x) { aout(self) << x << endl; }
        ;
    }
  };
}

template <class T>
class generator : public ref_counted {
public:
  /**
   * Returns how many credit is currently available.
   */
  size_t credit() const {
    return credit_;
  }

  /**
   * Grant new credit for sending messages.
   */
  size_t grant_credit(size_t x) {
    credit_ += x;
  }

  /**
   * Consumes up to `n` elements using the function object `f`.
   * @returns the number of consumed elements
   */
  virtual size_t consume(std::function<void (T&)> f, size_t num) = 0;

  /**
   * Returns whether this generator is closed.
   */
  bool closed() const {
    return closed_;
  }

  /**
   * Close this generator, i.e., transmit remaining items
   * and shutdown the stream aferwards.
   */
  void close() const {
    closed_ = true;
  }

private:
  size_t credit_;
  bool closed_;
};

/**
 * Represents a queue for outgoing data of type T.
 */
template <class T>
class output_queue : public generator<T> {
public:
  /**
   * Adds a new element to the queue.
   */
  void push(T x) {
    xs_.emplace_back(std::move(x));
  }

  /**
   * Adds `(first, last]` to the queue.
   */
  template <class Iter>
  void push(Iter first, Iter last) {
    xs_.insert(xs_.end(), first, last);
  }

  /**
   * Adds `xs` to the queue.
   */
  void push(std::initializer_list<T> xs) {
    push(xs.begin(), xs.end());
  }

  size_t size() const {
    return xs_.size();
  }

  size_t consume(std::function<void (T&)> f, size_t num) override {
    auto n = std::min({num, this->credit(), xs_.size()});
    auto first = xs_.begin();
    auto last = first + n;
    std::for_each(first, last, f);
    xs_.erase(first, last);
    return n;
  }

private:
  std::vector<T> xs_;
};

template <class T>
using output_queue_ptr = intrusive_ptr<output_queue<T>>;

template <class Container>
struct finite_generator {
public:
  using value_type = typename Container::value_type;
  using iterator = typename Container::iterator;

  finite_generator(Container&& xs) : xs_(std::move(xs)) {
    pos_ = xs_.begin();
    end_ = xs_.end();
  }

  optional<value_type> operator()() {
    if (pos_ == end_)
      return none;
    return std::move(*pos_++);
  }

private:
  Container xs_;
  iterator pos_;
  iterator end_;
};

enum some_error : uint8_t {
  queue_full = 1
};

error make_error(some_error x) {
  return {static_cast<uint8_t>(x), atom("some-error")};
}

constexpr int threshold = 10;

behavior edge_actor(event_based_actor*, output_queue_ptr<int> q) {
  return {
    [=](int task) -> result<void> {
      // drop message if no 
      if (q->size() >= threshold) {
        return some_error::queue_full;
      q->push(task);
      }
      return unit;
    }
  };
}

template <class Container>
finite_generator<Container> make_generator(Container xs) {
  return {std::move(xs)};
}

void bar(event_based_actor_* self) {
  static_assert(is_stream<stream<std::string>>::value, "...");
  std::vector<int> xs{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto f1 = self->spawn(foo1);
  //auto composed = self->spawn(foo2) * f1;
  auto single = self->spawn(foo3);
  self->stream(single, make_generator(xs)).then(
    [=] {
      aout(self) << "stream is done" << endl;
    }
  );
}

void test1(actor_system& system, const config&) {

  system.spawn(bar);
  system.await_all_actors_done();

  using std::make_pair;
  auto v0 = make_counted<ipair_pipe>();
  auto v1 = make_counted<stage1>(v0);
  auto l = make_counted<test_consumer>();
  auto ch = make_counted<streaming::pipeline<std::pair<int, int>, int>>(v0, v1, l);

  cout << "push without providing credit first" << endl;
  ch->push({make_pair(2, 10), make_pair(0, 10), make_pair(10, 0)});
  cout << "provide 12 credit (read all)" << endl;
  ch->grant_credit(12);
  cout << "provide 20 more credit" << endl;
  ch->grant_credit(20);
  cout << "push 5 elements" << endl;
  ch->push({make_pair(5, 50)});
  cout << "push 15 elements" << endl;
  ch->push({make_pair(15, 10)});
  cout << "push 20 elements" << endl;
  ch->push({make_pair(10, 1), make_pair(10, 2)});
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "close stream" << endl;
  ch->close();
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "grant 5 credit" << endl;
  ch->grant_credit(5);
  cout << "grant 6 credit" << endl;
  ch->grant_credit(6);

  streaming::stream_builder<int, int> sb;

  auto x = sb | [](int x) { return x * 2.0; }
              | [](double x) { return x < 1.; }
              | [](double x) { return std::to_string(x); };

  cout << "x(10) = " << eval(x, 10) << endl;

  cout << "x(10) = " << eval(x, 0) << endl;

  auto y = sb | [](int x) { return x % 2 == 1; };

  cout << "y(3) = " << eval(y, 3) << endl;
  cout << "y(4) = " << eval(y, 4) << endl;
}

// handshakes 'n stuff

#endif // 0

struct stream_id {
  strong_actor_ptr origin;
  uint64_t nr;
};

bool operator==(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin && x.nr == y.nr;
}

bool operator<(const stream_id& x, const stream_id& y) {
  return x.origin == y.origin ? x.nr < y.nr : x.origin < y.origin;
}

} // namespace <anonymous>

namespace std {
template <>
struct hash<stream_id> {
  size_t operator()(const stream_id& x) const {
    static_assert(sizeof(size_t) == sizeof(void*),
                  "platform not supported");
    auto tmp = reinterpret_cast<size_t>(x.origin.get())
               ^ static_cast<size_t>(x.nr);
    return tmp;
  }
};
}

namespace {

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_id& x) {
  return f(meta::type_name("stream_id"), x.origin, x.nr);
}

/// Initiates a stream handshake
struct stream_open {
  stream_id sid;
  std::vector<strong_actor_ptr> next_stages;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_open& x) {
  return f(meta::type_name("stream_open"), x.sid, x.next_stages);
}

enum class stream_demand_signaling_policy {
  credit,
  rate
};

/// Finalizes a stream handshake and signalizes initial demand.
struct stream_ack {
  stream_id sid;
  stream_demand_signaling_policy policy;
  int32_t initial_demand;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_ack& x) {
  return f(meta::type_name("stream_ack"), x.sid, x.policy,
           x.initial_demand);
}

/// Closes a stream.
struct stream_close {
  stream_id sid;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_close& x) {
  return f(meta::type_name("stream_close"), x.sid);
}

/// Signalizes demand from a sink to its source. The receiver interprets
/// the value according to the stream demand signaling policy for
/// the stream ID.
struct stream_demand {
  stream_id sid;
  int32_t value;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_demand& x) {
  return f(meta::type_name("stream_demand"), x.sid, x.value);
}

struct stream_batch {
  stream_id sid;
  std::vector<message> xs;
};

template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, stream_batch& x) {
  return f(meta::type_name("stream_batch"), x.sid, x.xs);
}

/*
using stream_msg = variant<none_t, stream_open, stream_ack,
                           stream_batch, stream_demand>;

behavior foo() {
  return {
    [](int) {
      return stream_msg{stream_demand{stream_id{}, 0}};
    }
  };
}
*/

/// A stream consumer handles upstream input.
class abstract_consumer : public ref_counted {
public:
  virtual ~abstract_consumer() {}

  /// Called after consuming the final element of a stream.
  virtual void on_complete() {}

protected:
  /// Stores an item in the output buffer, potentially sending it
  /// immediately if sufficient credit is available.
  virtual void push_message(message) = 0;
};

/// 
template <class T>
class consumer : public abstract_consumer {
public:
  virtual void push(T) = 0;

protected:
  void push_message(message x) override final {
    if (x.match_elements<T>())
      push(std::move(x.get_mutable_as<T>(0)));
    else
      CAF_LOG_ERROR("unexpected message in consumer");
  }
};

template <class T>
using consumer_ptr = intrusive_ptr<consumer<T>>;

/// A joint that grants write access to a buffered generator.
template <class T>
class joint : public consumer<T> {
public:
  /// Close this joint. Closing the last joint also closes the parent.
  virtual void close() = 0;

  /// Returns the currently available credit, i.e., work items that
  /// can be sent immediately without buffering. A negative credit
  /// means that the actor has buffered items but not enough credit
  /// to send them.
  virtual int credit() = 0;
};

/// A smart pointer to a `joint`.
template <class T>
using joint_ptr = intrusive_ptr<T>;

/// ```
/// abstract_consumer
/// | * internal interface, consumes a stream of `message` objects
/// | * member functions:
/// |   - push_message(): consume next element of a stream batch
/// |   - demand(): query capacity (signalized upstream)
/// ├─ consumer<T>
/// |  | * consumes a stream of `T` objects
/// |  | * member functions:
/// |  |   - push(T): consumes next element of a stream batch in its proper type
/// |  ├─ joint<T>
/// |  |  | * a connection to a buffered generator for pushing elements into a
/// |  |  |   stream, i.e., a joint turns a buffered generator into a consumer
/// |  |  | * member functions:
/// |  |  |   - close(): closes this joint; generators close if no joint is left
/// |  |  |   - credit(): query available downstream capacity
/// |  |  |   - push(T): appends a new tuple to the generator's buffer
/// |  ├─ y_connector<T>
/// |  |  | * connects to two or more downstream consumer (joints)
///
/// abstract_generator
/// | * internal interface, generates a stream of `message` objects
/// | * member functions:
/// |   - closed(): signalize CAF the stream reached its end
/// |   - pull_message(): get next item for a stream batch
/// |   - credit(): query available downstream capacity
/// ├─ generator<T>
/// |  | * generic, user-facing interface
/// |  | * member functions:
/// |  |   - pull(): get next item for the stream batch in its proper type
/// |  ├─ buffered_generator
/// |  |  * full control over emitted tuples via joints
/// |  |  * member functions:
/// |  |    - make_joint(): returns a new joint connected to this generator
/// |  ├─ transforming_generator:
/// |  |  * emits tuples based on user-generated tranformation
/// ```
///
/// Use cases:
/// - Simple Pipeline: A -> B -> C
///   + B consumes tuples from A
///   + B generates tuples for C
///   + Example: Text processing
/// - Merging: (A1 || A2) -> B -> C
///   + B consumes tuples from A1 and A2 (independent streams)
///   + B generates tuples for C
///   + Example: Load balancing on the edge (e.g. A1 and A2 model HTTP clients),
///     where A1 and A2 drop incoming requests when running out of credit
///   + B splits its capacity between A1 and A2
/// - Splitting: A -> B -> (C1 || C2)
///   + B consumes tuples from A
///   + B generates tuples for C1 and C2 (independent streams)
///   + Example: Load balancing of workers; B dispatches tuples to either C1
///     or C2 depending on available capacity
///   + B joins capacity of C1 and C2, announcing merged demand to A
///
/// # Simple Pipeline
///
/// A uses a generator for emitting tuples downstream. B uses a
/// buffered_generator with one joint. Finally, C uses a consumer,
/// reducing the entire stream into a single result (possibly void).
///
/// # Merging
///
/// B uses a buffered_generator for emitting tuples downstream. This
/// generator has two joints that consume tuples from A1 and A2 by
/// writing into the buffer.
///
/// # Splitting
///
/// B uses a y_connector multplexing between two joints
/// (connected to C1 and C2).
class abstract_generator : public ref_counted {
public:
  abstract_generator() : credit_(0) {
    // nop
  }
  virtual ~abstract_generator() {}
  virtual bool closed() const = 0;
  virtual message pull_message() = 0;
  inline int credit() const {
    return credit_;
  }

  int inc_credit(int amount = 1) {
    return credit_ += amount;
  }

  int dec_credit(int amount = 1) {
    CAF_ASSERT(amount >= credit_);
    return credit_ -= amount;
  }

private:
  int credit_;
};

using abstract_generator_ptr = intrusive_ptr<abstract_generator>;

template <class T>
class generator : public abstract_generator {
public:
  virtual optional<T> pull() = 0;
  message pull_message() {
    auto x = pull();
    if (x)
      return make_message(std::move(*x));
    return {};
  }
};

/// A smart pointer to a generator.
template <class T>
using generator_ptr = intrusive_ptr<generator<T>>;

template <class T>
class buffered_generator : public generator<T> {
public:
  class joint_impl;

  friend class joint_impl;

  buffered_generator() : open_joints_(0) {
    // nop
  }

  joint_ptr<T> make_joint() {
    return new joint_impl(this);
  }

  bool closed() const override {
    return open_joints_ == 0;
  }

  optional<T> pull() override {
    optional<T> result;
    if (!buf_.empty()) {
      result = std::move(buf_.front());
      buf_.pop_front();
    }
    return result;
  }

  class joint_impl final : public joint<T> {
  public:
    using parent_ptr = intrusive_ptr<buffered_generator>;

    joint_impl(parent_ptr parent) : parent_(std::move(parent)) {
      // nop
    }

    void close() override {
      parent_->open_joints_ -= 1;
    }

    int credit() override {
      return parent_->credit()
             - static_cast<int>(parent_->buf_.size());
    }

    void push(T x) override {
      parent_->buf_.emplace_back(std::move(x));
    }

  private:
    parent_ptr parent_;
  };

private:
  size_t open_joints_;
  std::deque<T> buf_;
};

template <class T>
using buffered_generator_ptr = intrusive_ptr<buffered_generator<T>>;

template <class In, class Out>
using stage = std::pair<consumer_ptr<In>, generator_ptr<Out>>;

using abstract_consumer_ptr = intrusive_ptr<abstract_consumer>;

template <class Out>
using half_erased_stage = std::pair<abstract_consumer_ptr, generator_ptr<Out>>;

using abstract_stage = std::pair<intrusive_ptr<abstract_consumer>,
                                 intrusive_ptr<abstract_generator>>;

template <class T>
class stream {
public:
  using value_type = variant<none_t, generator_ptr<T>, half_erased_stage<T>>;

  stream() = default;
  stream(stream&&) = default;
  stream(const stream& x) = default;

  stream(generator_ptr<T> x) : value_(std::move(x)) {
    // nop
  }

  template <class In>
  stream(stage<In, T> x) : value_(std::make_pair(abstract_consumer_ptr{std::move(x.first)}, std::move(x.second))) {
    // nop
  }

  value_type& value() {
    return value_;
  }
  
private:
  value_type value_;
};

} // namespace <anonymous>

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(stream<int>)

namespace {

class streamer : public event_based_actor {
public:
  template <class U, class T, class F>
  output_stream<U> stage(const stream<T>, F) {
    // nop
  }
};

#define debug(x)                                                               \
  aout(self) << "[id: " << self->id() << "] "                                  \
             << self->current_mailbox_element()->content() << std::endl

template <class f, class g>
constexpr bool HasSignature() {
  return std::is_same<f, signature_t<g>>::value;
}

template <class State, class T, class Init, class Pull, class Closed>
generator_ptr<T> make_generator(Init init, Pull pull, Closed closed) {
  static_assert(HasSignature<void (State&), Init>(),
                "first argument must have have signature 'void (state&)'");
  static_assert(HasSignature<optional<T> (State&), Pull>(),
                "second argument must have signature 'optional<T> (state&)'");
  static_assert(HasSignature<bool (const State&), Closed>(),
                "third argument must have signature 'bool (state&)'");
  class impl final : public generator<T> {
  public:
    impl(Pull f1, Closed f2) : pull_(f1), closed_(f2) {
      // nop
    }
    optional<T> pull() override {
      return pull_(state_);
    }
    bool closed() const override {
      return closed_(state_);
    }
    State& state() {
      return state_;
    }
  private:
    State state_;
    Pull pull_;
    Closed closed_;
  };
  auto result = make_counted<impl>(std::move(pull), std::move(closed));
  init(result->state());
  return result;
}

template <class State, class In, class Out,
          class Init, class Transform, class OnComplete>
stage<In, Out> make_stage(Init init, Transform transform, OnComplete on_complete) {
  class consumer_impl final : public consumer<In> {
  public:
    using fun = Transform;
    using parent_ptr = buffered_generator_ptr<Out>;
    consumer_impl(parent_ptr parent, Transform f0, OnComplete f1)
      : joint_(std::move(parent)),
        f_(std::move(f0)),
        on_complete_(std::move(f1)) {
      // nop
    }
    void push(In x) override {
      f_(state_, joint_, std::move(x));
    }
    void on_complete() override {
      // nop
    }
    State& state() {
      return state_;
    }
  private:
    State state_;
    typename buffered_generator<Out>::joint_impl joint_;
    fun f_;
    OnComplete on_complete_;
  };
  auto g = make_counted<buffered_generator<Out>>();
  auto s = make_counted<consumer_impl>(g, std::move(transform),
                                       std::move(on_complete));
  init(s->state());
  return std::make_pair(std::move(s), std::move(g));
}

template <class State, class In, class Out, class Transform>
stage<In, Out> make_stage(Transform f) {
  return make_stage<State, In, Out>([](State&) { }, f, [](State&) { });
}

behavior file_reader() {
  return {
    [=](const std::string& str) -> stream<int> {
      return make_generator<std::ifstream, int>(
        [&](std::ifstream& f) {
          f.open(str);
        },
        [](std::ifstream& f) -> optional<int> {
          int res;
          if (f >> res)
            return res;
          return none;
        },
        [](const std::ifstream& f) {
          return f.bad() || f.eof();
        }
      );
    }
  };
}

behavior filter_odd_nums() {
  return {
    [=](stream<int>) -> stream<int> {
      return make_stage<unit_t, int, int>(
        [](unit_t&, joint<int>& out, int x) {
          if (x % 2 == 0)
            out.push(x);
        }
      );
    }
  };
}

struct stream_source_state {
  stream_id sid;
  abstract_generator_ptr gptr;
  strong_actor_ptr next_stage;
  size_t open_credit;

  void transmit(scheduled_actor* self) {
    CAF_ASSERT(self != nullptr);
    CAF_ASSERT(next_stage != nullptr);
    auto s = open_credit;
    aout(self) << "[id: " << self->id() << "] available credit: " << s << std::endl;
    if (s > 0) {
      open_credit -= s;
      std::vector<message> batch;
      for (size_t i = 0; i < s && !gptr->closed(); ++i) {
        auto tmp = gptr->pull_message();
        if (!tmp.empty())
          batch.push_back(std::move(tmp));
      }
      aout(self) << "[id: " << self->id() << "] transmit batch: "
                 << deep_to_string(batch) << std::endl;
		  auto close_stream = [&] {
        next_stage->enqueue(self->ctrl(), message_id::make(),
                            make_message(stream_close{sid}),
                            self->context());
      };
      if (!batch.empty()) {
        next_stage->enqueue(self->ctrl(), message_id::make(),
                            make_message(stream_batch{sid, std::move(batch)}),
                            self->context());
        if (gptr->closed())
          close_stream();
      } else {
        close_stream();
      }
    }
  }
};

struct src_state {
  std::unordered_map<stream_id, stream_source_state> data;
};

behavior source(stateful_actor<src_state>* self) {
  auto bhvr = file_reader();
  return {
    [=](add_atom, actor new_sink, std::string& fname) mutable {
      debug();
      stream_id sid{self->ctrl(), 0};
      self->send(new_sink, stream_open{sid, {}});
      auto& st = self->state;
      auto fname_msg = make_message(std::move(fname));
      auto res = bhvr(fname_msg);
      if (!res) {
        CAF_LOG_ERROR("failed to init stream with message");
        return;
      }
      if (!res->match_elements<stream<int>>()) {
        CAF_LOG_ERROR("expected a stream<int> from handler, got: " << CAF_ARG(res));
        return;
      }
      auto ptr = get<generator_ptr<int>>(&res->get_mutable_as<stream<int>>(0).value());
      if (!ptr) {
        CAF_LOG_ERROR("stream not initialized using a generator");
        return;
      }
      st.data.emplace(sid,
                      stream_source_state{sid, std::move(*ptr),
                                          actor_cast<strong_actor_ptr>(new_sink), 0});
    },
    [=](const stream_ack& x) {
      debug();
      auto i = self->state.data.find(x.sid);
      if (i != self->state.data.end()) {
        i->second.open_credit = x.initial_demand;
        i->second.transmit(self);
        if (i->second.gptr->closed())
          self->state.data.erase(i);
      }
    },
    [=](const stream_demand& x) {
      debug();
      auto i = self->state.data.find(x.sid);
      if (i != self->state.data.end()) {
        i->second.open_credit += static_cast<size_t>(x.value);
        i->second.transmit(self);
        if (i->second.gptr->closed())
          self->state.data.erase(i);
      }
    }
  };
}

struct stream_sink_state {
  stream_id sid;
  strong_actor_ptr upstream;
  int32_t low_watermark;
  int32_t assigned_credit;
  int32_t max_credit;
  void send_demand(scheduled_actor* self, int32_t released_credit) {
    if (max_credit > 0) {
      assigned_credit -= released_credit;
      if (assigned_credit < low_watermark) {
        assigned_credit += 10;
        upstream->enqueue(self->ctrl(), message_id::make(),
                          make_message(stream_demand{sid, 10}),
                          self->context());
      }
    }
  }
  void source_closed() {
    low_watermark = assigned_credit = max_credit = 0;
    upstream = nullptr;
  }
};

struct snk_state {
  std::unordered_map<stream_id, stream_sink_state> data;
};

behavior sink(stateful_actor<snk_state>* self) {
  return {
    [=](const stream_open& x) -> result<stream_ack> {
      debug();
      auto upstream = self->current_sender();
      if (!upstream)
        return sec::invalid_argument;
      auto& st = self->state;
      auto i = st.data.find(x.sid);
      if (i == st.data.end()) {
        i = st.data.emplace(x.sid, stream_sink_state{x.sid, upstream, 7, 10, 25}).first;
        return stream_ack{x.sid, stream_demand_signaling_policy::credit, 5};
      }
      return sec::invalid_argument;
    },
    [=](const stream_batch& x) {
      debug();
      auto& st = self->state;
      auto i = st.data.find(x.sid);
      if (i != st.data.end())
        i->second.send_demand(self, static_cast<int32_t>(x.xs.size()));
    },
    [=](const stream_close& x) {
      debug();
      auto i = self->state.data.find(x.sid);
      if (i != self->state.data.end()) {
        i->second.source_closed();
      }
    }
  };
}

void test2(actor_system& sys, const config&) {
  auto src = sys.spawn(source);
  auto snk = sys.spawn(sink);
  anon_send(src, add_atom::value, snk, "test.txt");
}

void caf_main(actor_system& sys, const config& cfg) {
  //test1(sys, cfg);
  test2(sys, cfg);
}

} // namespace <anonymous>

CAF_MAIN()
