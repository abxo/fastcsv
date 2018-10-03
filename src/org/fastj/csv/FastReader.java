package org.fastj.csv;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

/**
 * Fast CSV Reader
 * 
 * @author zhou
 *
 */
final class FastReader implements Closeable {

	private static final int SIZE = 8192;

	private char[] buf = new char[SIZE];
	private Reader r;
	private int cnt, nc;

	public FastReader(Reader reader) {
		this.r = reader;
	}

	CharBuf readLine(CharBuf cbuf) throws IOException {
		if (cbuf != null) {
			cbuf.reset();
		}

		CharBuf chs = cbuf;
		int startChar;

		synchronized (this) {
			byte bit = 0x00;
			for (;;) {
				if (nc >= cnt)
					fill();

				if (nc >= cnt) {
					if (chs != null && chs.length() > 0)
						return chs;
					else
						return null;
				}
				boolean eol = false;
				char c = 0;
				int i;

				for (i = nc; i < cnt; i++) {
					c = buf[i];
					if (c > '"' || c < 0) {
						continue;
					}
					if (c == '"') {
						bit ^= 0x01;
						continue;
					}
					if ((c == '\n') || (c == '\r')) {
						if (bit == 0) {
							eol = true;
							break;
						}
					}
				}

				startChar = nc;
				nc = i;

				if (eol) {
					if (chs == null) {
						chs = new CharBuf(i - startChar);
						chs.append(buf, startChar, i - startChar);
					} else {
						chs.append(buf, startChar, i - startChar);
					}
					nc++;

					if (nc >= cnt)
						fill();

					if (c == '\r' && (buf[nc] == '\n')) {
						nc++;
					}

					return chs;
				}

				if (chs == null)
					chs = new CharBuf(256);
				chs.append(buf, startChar, i - startChar);
			}
		}
	}

	private void fill() throws IOException {
		int n = r.read(buf, 0, buf.length);
		while (n == 0) {
			n = r.read(buf, 0, buf.length);
		}

		if (n > 0) {
			cnt = n;
			nc = 0;
		}
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			if (r == null)
				return;
			try {
				r.close();
			} finally {
				r = null;
				buf = null;
			}
		}
	}

}
