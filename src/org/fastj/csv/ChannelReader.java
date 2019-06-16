package org.fastj.csv;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;

public class ChannelReader implements Closeable {
	private static final int SIZE = 8192 * 4;

	private byte[] buf = new byte[SIZE];
	private FileChannel fch;
	private int cnt, nc;
	private long start;
	private long size;
	private long readLen = 0;
	private final ByteBuf bbuf = ByteBuf.POOL.get();
	private final ByteBuffer cache = ByteBuffer.wrap(buf, 0, buf.length);
	private final CharsetDecoder decoder;

	public ChannelReader(String file, long start, long size, int skipLine) throws IOException {
		this(file, start, size, skipLine, StandardCharsets.UTF_8);
	}

	public ChannelReader(String file, long start, long size, int skipLine, Charset charset) throws IOException {
		this.fch = FileChannel.open(new File(file).toPath());
		this.start = start;
		this.size = size;
		this.decoder = charset != null ? charset.newDecoder() : StandardCharsets.UTF_8.newDecoder();
		this.readLen = 0;
		this.fch.position(start);
		if (skipLine >= 0) {
			synchronized (this) {
				while (skipLine-- >= 0) {
					skipLine();
					readLen = fch.position() - cnt + nc - start;
				}
			}
		}
	}

	CharBuf readLine(CharBuf cbuf) throws IOException {
		synchronized (this) {
			ByteBuf buf = readLine0();
			if (buf == null) {
				return null;
			}
			readLen = fch.position() - cnt + nc - start;
			cbuf = b2c(buf, cbuf);
			return cbuf;
		}
	}

	private ByteBuf readLine0() throws IOException {
		if (readLen > size) {
			return null;
		}

		ByteBuf chs = bbuf;
		chs.reset();
		int startChar;

		byte bit = 0x00;
		for (;;) {
			if (nc >= cnt)
				fill();

			if (nc >= cnt) {
				if (chs.length() > 0)
					return chs;
				else
					return null;
			}
			boolean eol = false;
			byte c = 0;
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
				chs.append(buf, startChar, i - startChar);
				nc++;

				if (nc >= cnt)
					fill();

				if (c == '\r' && (buf[nc] == '\n')) {
					nc++;
				}

				return chs;
			}

			chs.append(buf, startChar, i - startChar);
		}
	}

	void skipLine() throws IOException {
		ByteBuf chs = bbuf;
		chs.reset();
		int startChar;

		byte bit = 0x00;
		for (;;) {
			if (nc >= cnt)
				fill();

			if (nc >= cnt) {
				if (chs.length() > 0)
					return;
				else
					return;
			}
			boolean eol = false;
			byte c = 0;
			int i;

			for (i = nc; i < cnt; i++) {
				c = buf[i];
				if (c > 13 || c < 0) {
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
				chs.append(buf, startChar, i - startChar);
				nc++;

				if (nc >= cnt)
					fill();

				if (c == '\r' && (buf[nc] == '\n')) {
					nc++;
				}

				return;
			}

			chs.append(buf, startChar, i - startChar);
		}
	}

	private CharBuf b2c(ByteBuf bbuf, CharBuf cbuf) throws IOException {
		cbuf = cbuf == null ? CharBuf.POOL.get() : cbuf;
		cbuf.reset();
		ByteBuffer bb = ByteBuffer.wrap(bbuf.value, 0, bbuf.count);
		cbuf.ensureCapacity(bbuf.count);
		CharBuffer cb = CharBuffer.wrap(cbuf.value, 0, cbuf.capacity);
		cb.clear();

		CoderResult cr = decoder.decode(bb, cb, true);
		if (cr.isError()) {
			cr.throwException();
		}
		cbuf.count = cb.position();

		return cbuf;
	}

	private void fill() throws IOException {
		cache.clear();
		int n = fch.read(cache);
		while (n == 0) {
			n = fch.read(cache);
		}
		if (n > 0) {
			cnt = n;
			nc = 0;
		}
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			if (fch == null)
				return;
			try {
				fch.close();
				ByteBuf.POOL.release(bbuf);
			} finally {
				fch = null;
				buf = null;
			}
		}
	}
}
