package org.fastj.csv;

import java.util.Arrays;

import org.fastj.csv.ObjectPool.ObjectFactory;

public final class ByteBuf {
	static ObjectPool<ByteBuf> POOL = new ObjectPool<>();
	static {
		POOL.setFacory(new ObjectFactory<ByteBuf>() {
			public ByteBuf create() {
				return new ByteBuf(1024);
			}
		});
	}

	int count;
	byte[] value;
	int capacity;

	ByteBuf(int capacity) {
		value = new byte[capacity];
		this.capacity = value.length;
	}

	public int length() {
		return count;
	}

	public void reset() {
		count = 0;
	}

	public ByteBuf copy() {
		ByteBuf copy = POOL.get();
		copy.reset();
		copy.append(value, 0, count);
		return copy;
	}

	public void append(final byte c) {
		int nlen = count + 1;
		if (capacity < nlen) {
			ensureCapacity(nlen);
		}
		value[count++] = c;
	}

	public ByteBuf append(byte str[], int offset, int len) {
		int nlen = count + len;
		if (capacity < nlen) {
			ensureCapacity(nlen);
		}
		System.arraycopy(str, offset, value, count, len);
		count = nlen;
		return this;
	}

	void ensureCapacity(int nc) {
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
