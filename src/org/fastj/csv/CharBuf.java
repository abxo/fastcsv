package org.fastj.csv;

import java.util.Arrays;

import org.fastj.csv.ObjectPool.ObjectFactory;

public final class CharBuf {
	public static final ObjectPool<CharBuf> POOL = new ObjectPool<>();
	static {
		POOL.setFacory(new ObjectFactory<CharBuf>() {
			public CharBuf create() {
				return new CharBuf(1024);
			}
		});
	}

	int idx;
	int count;
	char[] value;
	int capacity;

	CharBuf(int capacity) {
		value = new char[capacity];
		this.capacity = value.length;
	}

	public int length() {
		return count;
	}

	public char[] get() {
		return value;
	}

	public int idx() {
		return idx;
	}

	public CharBuf idx(int idx) {
		this.idx = idx;
		return this;
	}

	public String toStringAndReset() {
		if (count > 0) {
			final String s = new String(value, 0, count);
			count = 0;
			return s;
		}
		return "";
	}

	public void reset() {
		count = 0;
	}

	public CharBuf copy() {
		CharBuf copy = POOL.get();
		copy.reset();
		copy.append(value, 0, count);
		copy.idx = idx;
		return copy;
	}

	public boolean hasContent() {
		return count > 0;
	}

	public void append(final char c) {
		int nlen = count + 1;
		if (capacity < nlen) {
			ensureCapacity(nlen);
		}
		value[count++] = c;
	}

	public CharBuf append(char[] str, int offset, int len) {
		int nlen = count + len;
		if (capacity < nlen) {
			ensureCapacity(nlen);
		}
		System.arraycopy(str, offset, value, count, len);
		count = nlen;
		return this;
	}

	public void ensureCapacity(int nc) {
		int cc = value.length;
		if (nc > cc) {
			int newSize = cc << 1 + 2;
			while (nc > newSize) {
				newSize = newSize << 1 + 2;
			}
			value = Arrays.copyOf(value, newSize);
			capacity = newSize;
		}
	}

}
