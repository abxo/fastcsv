package org.fastj.csv;

import java.util.concurrent.locks.LockSupport;

/**
 * Simple RingBuffer(Not Thread Safe, 1 reader 1 writer)
 * 
 * @author zhou
 *
 * @param <T>
 */
public class RingBuffer<T> {

	private volatile int wpc;
	private final int mask;
	// L1 Cache line padding
	long p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15;
	private volatile int rpc;
	private volatile boolean finish = false;
	private volatile Object[] queue;
	int failCnt = 0;

	public RingBuffer(int size) {
		if ((size & (size - 1)) != 0) {
			throw new IllegalArgumentException("size must be a power of 2!");
		}
		queue = new Object[size];
		wpc = rpc = 0;
		mask = size - 1;
	}

	public boolean add(T t) {
		if (t == null) {
			throw new NullPointerException();
		}

		if (finish) {
			throw new IllegalStateException("RB is finished.");
		}

		if (isFull()) {
			return false;
		}

		queue[wpc] = t;
		wpc = (wpc + 1) & mask;
		return true;
	}

	public T get() {
		for (; isEmpty();) {
			if (finish) {
				return null;
			}
			failCnt++;
			LockSupport.parkNanos(1);
		}
		T t = (T) queue[rpc];
		rpc = (rpc + 1) & mask;
		return t;
	}

	public void finish() {
		finish = true;
	}

	private final boolean isEmpty() {
		return rpc == wpc;
	}

	private final boolean isFull() {
		return ((wpc + 1) & mask) == rpc;
	}
}
