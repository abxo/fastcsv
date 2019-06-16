package org.fastj.csv;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

final class ObjectPool<T> {

	interface ObjectFactory<T> {
		T create();
	}

	private AtomicLong idx = new AtomicLong(0);
	private LinkedTransferQueue<T> queue = new LinkedTransferQueue<>();
	private ObjectFactory<T> factory;

	private int max = 10240;

	public void setFacory(ObjectFactory<T> objf) {
		this.factory = objf;
	}

	public T get() {
		T t = queue.poll();

		if (t != null) {
			return t;
		}

		if (idx.get() < max) {
			idx.incrementAndGet();
			return factory.create();
		} else {
			try {
				t = queue.poll(1, TimeUnit.MILLISECONDS);
				if (t != null) {
					return t;
				}
			} catch (Exception e) {
			}
		}

		idx.incrementAndGet();
		return factory.create();
	}

	public void release(T t) {
		queue.offer(t);
	}

	public long getIdx() {
		return idx.get();
	}

	public int size() {
		return queue.size();
	}

}
