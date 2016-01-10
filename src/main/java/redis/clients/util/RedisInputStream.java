/*
 * Copyright 2009-2010 MBTE Sweden AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis.clients.util;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisInputStream extends BufferedInputStream {

    public RedisInputStream(InputStream in, int size) {
	super(in, size);
    }

    public RedisInputStream(InputStream in) {
	this(in, 8192);
    }

	public synchronized byte readByte() throws IOException {
		int b = read();
		if (b == -1) {
			throw new EOFException("Was expecting to get a byte, but got the end of stream");
		}
		return (byte)b;
	}

    public synchronized String readLine() {
	int b;
	int c;
	StringBuilder sb = new StringBuilder();

	try {
	    while (true) {

		b = read();
			if (b == -1) {
				break;
			}
		if (b == '\r') {

		    c = read();
			if (c == -1) {
				sb.append((char) b);
				break;
			}
		    if (c == '\n') {
			break;
		    }
		    sb.append((char) b);
		    sb.append((char) c);
		} else {
		    sb.append((char) b);
		}
	    }
	} catch (IOException e) {
	    throw new JedisConnectionException(e);
	}
	String reply = sb.toString();
	if (reply.length() == 0) {
	    throw new JedisConnectionException(
		    "It seems like server has closed the connection.");
	}
	return reply;
    }
}
