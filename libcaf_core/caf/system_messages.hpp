/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2016                                                  *
 * Dominik Charousset <dominik.charousset (at) haw-hamburg.de>                *
 *                                                                            *
 * Distributed under the terms and conditions of the BSD 3-Clause License or  *
 * (at your option) under the terms and conditions of the Boost Software      *
 * License 1.0. See accompanying files LICENSE and LICENSE_ALTERNATIVE.       *
 *                                                                            *
 * If you did not receive a copy of the license files, see                    *
 * http://opensource.org/licenses/BSD-3-Clause and                            *
 * http://www.boost.org/LICENSE_1_0.txt.                                      *
 ******************************************************************************/

#ifndef CAF_SYSTEM_MESSAGES_HPP
#define CAF_SYSTEM_MESSAGES_HPP

#include <vector>
#include <cstdint>
#include <type_traits>

#include "caf/fwd.hpp"
#include "caf/group.hpp"
#include "caf/actor_addr.hpp"
#include "caf/deep_to_string.hpp"

#include "caf/meta/type_name.hpp"

#include "caf/detail/tbind.hpp"
#include "caf/detail/type_list.hpp"

namespace caf {

/// Sent to all links when an actor is terminated.
/// @note Actors can override the default handler by calling
///       `self->set_exit_handler(...)`.
struct exit_msg {
  /// The source of this message, i.e., the terminated actor.
  actor_addr source;

  /// The exit reason of the terminated actor.
  error reason;
};

/// @relates exit_msg
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, exit_msg& x) {
  return f(meta::type_name("exit_msg"), x.source, x.reason);
}

/// Sent to all actors monitoring an actor when it is terminated.
struct down_msg {
  /// The source of this message, i.e., the terminated actor.
  actor_addr source;

  /// The exit reason of the terminated actor.
  error reason;
};

/// @relates down_msg
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, down_msg& x) {
  return f(meta::type_name("down_msg"), x.source, x.reason);
}

/// Sent to all members of a group when it goes offline.
struct group_down_msg {
  /// The source of this message, i.e., the now unreachable group.
  group source;
};

/// @relates group_down_msg
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, group_down_msg& x) {
  return f(meta::type_name("group_down_msg"), x.source);
}

/// Signalizes a timeout event.
/// @note This message is handled implicitly by the runtime system.
struct timeout_msg {
  /// Actor-specific timeout ID.
  uint32_t timeout_id;
};

/// @relates timeout_msg
template <class Inspector>
typename Inspector::result_type inspect(Inspector& f, timeout_msg& x) {
  return f(meta::type_name("timeout_msg"), x.timeout_id);
}

/// Sources send a SYN message to initiate the
/// handshake for establishing a stream.
struct flow_control_syn {
  strong_actor_ptr source;
};

/// Sinks send a SYN-ACK message with initial credit to
/// the source after establishing the stream.
struct flow_control_syn_ack {

};

/// Sources close a stream by sending FIN to the sink.
struct flow_control_fin {
  strong_actor_ptr source;
};

/// Sinks propagates demand from the sink to the source.
struct flow_control_demand {
  size_t amount;
};

} // namespace caf

#endif // CAF_SYSTEM_MESSAGES_HPP
