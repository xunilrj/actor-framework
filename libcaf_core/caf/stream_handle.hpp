/******************************************************************************
 *                       ____    _    _____                                   *
 *                      / ___|  / \  |  ___|    C++                           *
 *                     | |     / _ \ | |_       Actor                         *
 *                     | |___ / ___ \|  _|      Framework                     *
 *                      \____/_/   \_|_|                                      *
 *                                                                            *
 * Copyright (C) 2011 - 2015                                                  *
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

#ifndef CAF_STREAM_HANDLE_HPP
#define CAF_STREAM_HANDLE_HPP

#include "caf/fwd.hpp"
#include "caf/actor.hpp"

namespace caf {

class stream_handle {
public:
  friend class local_actor;

  stream_handle();
  stream_handle(stream_handle&&) = default;
  stream_handle(const stream_handle&) = default;

private:
  stream_handle(local_actor* self, actor sink);

  local_actor* self_;
  actor sink_;
};

} // namespace caf

#endif // CAF_STREAM_HANDLE_HPP
