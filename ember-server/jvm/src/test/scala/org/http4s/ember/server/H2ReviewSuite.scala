/*
 * Copyright 2019 http4s.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.http4s.ember.server

import cats.effect._
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.Stream
import org.http4s._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.h2.H2Keys.Http2PriorKnowledge

import scala.concurrent.duration._

/** Exercises real HTTP/2 client/server exchanges for the unread-body deadlock.
  * Ignored request bodies must not stall large responses, even with timeouts
  * disabled. Uploads larger than the receive window must also survive delayed
  * body consumption without being mistaken for idle connections.
  */
class H2ReviewSuite extends Http4sSuite {
  private def exchange(
      app: HttpApp[IO],
      body: EntityBody[IO],
      expected: String,
      idleTimeout: Duration,
  ): IO[Unit] = {
    val server = EmberServerBuilder
      .default[IO]
      .withHost(ipv4"127.0.0.1")
      .withPort(port"0")
      .withHttp2
      .withIdleTimeout(idleTimeout)
      .withHttpApp(app)
      .build
    val client = EmberClientBuilder.default[IO].withHttp2.build
    (server, client).tupled.use { case (server, client) =>
      val uri = Uri.unsafeFromString(s"http://127.0.0.1:${server.addressIp4s.port.value}/")
      val req = Request[IO](Method.POST, uri)
        .withBodyStream(body)
        .withAttribute(Http2PriorKnowledge, ())
      client.expect[String](req).timeout(8.seconds).assertEquals(expected)
    }
  }

  List[(Int, Int, Duration)](
    (7, 300, 2.seconds),
    (100000, 1, 2.seconds),
    (100000, 300, 2.seconds),
    (100000, 300, Duration.Inf),
  ).foreach { case (responseSize, requestChunks, timeout) =>
    test(s"unread $requestChunks-chunk body with $responseSize-byte response (timeout=$timeout)") {
      val expected = "x" * responseSize
      val app =
        HttpApp[IO](_ => IO.sleep(200.millis).as(Response[IO](Status.Ok).withEntity(expected)))
      // Keep each byte in a separate chunk to exhaust the former bounded channel.
      val body = Stream.emits(List.fill(requestChunks)("a")).flatMap(s => Stream.emits(s.getBytes))
      exchange(app, body, expected, timeout)
    }
  }

  List(10000, 100000).foreach { size =>
    test(s"a delayed body reader accepts a $size byte upload beyond the idle timeout") {
      val app = HttpApp[IO] { req =>
        IO.sleep(2.seconds) >> req.body.compile.count.map { received =>
          Response[IO](Status.Ok).withEntity(received.toString)
        }
      }
      exchange(app, Stream.emits(Array.fill[Byte](size)(0)), size.toString, 500.millis)
    }
  }

  test("reproduce race condition".only) {
    val numIterations =
      10L // Since this is a race condition, we need to execute the codepath multiple times.
    val app = HttpApp[IO](_ => Response[IO](Status.Ok).pure[IO])
    val server = EmberServerBuilder
      .default[IO]
      .withHost(ipv4"127.0.0.1")
      .withPort(port"0")
      .withHttp2
      .withHttpApp(app)
      .build
    val client = EmberClientBuilder.default[IO].withHttp2.build
    (server, client).tupled.use { case (server, client) =>
      val uri = Uri.unsafeFromString(s"http://127.0.0.1:${server.addressIp4s.port.value}/")
      val req = Request[IO](Method.POST, uri)
        .withEntity("Somebody")
        .withAttribute(Http2PriorKnowledge, ())
      val program = (
        // Make two requests in parallel. One of them will be cancelled due to race.
        // This causes the client to send an RstStream cancel message.
        // Occasionally, the server may send data before it has received the message.
        // This causes the client to misbehave and stop any concurrent requests.
        client.expect[String](req).race(client.expect[String](req)),
        // Send a concurrent request to surface the exception.
        client.expect[String](req),
      ).parTupled.void
      Stream.repeatEval(program).take(numIterations).compile.drain
    }
  }
}
