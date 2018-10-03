package org.fastj.csv;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.fastj.csv.Util.*;

final class N2NMatrixReader {

	ChannelReader[] readers;
	NDistributor[] processors;
	String[] header;
	int olen;
	int[] vidx;
	int[] idxmap;

	N2NMatrixReader(String file, int rsize, int psizePerReader, int headline, String[] rcols) throws IOException {

		long fsize = Files.size(new File(file).toPath());
		long blockSize = fsize / rsize + 1;

		readers = new ChannelReader[rsize];
		processors = new NDistributor[rsize];
		for (int i = 0; i < rsize; i++) {
			readers[i] = new ChannelReader(file, blockSize * i, blockSize, i != 0 ? 0 : -1);
			processors[i] = new NDistributor(psizePerReader, 64);
		}

		ChannelReader fhr = readers[0];
		CharBuf hl = null;
		while (headline-- >= 0) {
			hl = fhr.readLine(hl);
		}

		header = parseSimple(hl); // throw NPE if headline < 0
		CharBuf.POOL.release(hl);
		vidx = createVidx(header, rcols);
		idxmap = createIdxMap(header.length, vidx);
		olen = vidx.length;
		header = rcols != null ? rcols : header;
	}

	double[][] get() {
		final CountDownLatch cdl = new CountDownLatch(readers.length);

		PNode[] tmpRlts = new PNode[readers.length];
		for (int i = 0; i < readers.length; i++) {
			final int pc = i;
			Util.executor.execute(() -> {
				try {
					double[][] data = readCsv(readers[pc], processors[pc]);
					tmpRlts[pc] = new PNode(data);
				} finally {
					cdl.countDown();
				}
			});
		}

		try {
			cdl.await();
		} catch (InterruptedException e) {
			return null;
		}

		int total = 0;

		for (PNode pn : tmpRlts) {
			total += pn.data.length;
		}

		double[][] data = new double[total][];

		int pc = 0;
		for (PNode pn : tmpRlts) {
			for (double[] row : pn.data) {
				data[pc++] = row;
			}
		}

		return data;
	}

	double[][] readCsv(ChannelReader reader, NDistributor processor) {
		processor.start();
		CharBuf buf = CharBuf.POOL.get();
		double[][] data = new double[0][0];

		try (ChannelReader r = reader) {
			CharBuf line = buf;
			int idx = 0;
			while ((line = r.readLine(line)) != null) {
				processor.put(line.copy().idx(idx++));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CharBuf.POOL.release(buf);
			processor.finish();
		}

		data = processor.get();
		return data;
	}

	class PNode {
		double[][] data;

		PNode(double[][] data) {
			this.data = data;
		}
	}

	class DNode {
		int idx;
		double[] data;

		DNode(int idx, double[] data) {
			this.idx = idx;
			this.data = data;
		}
	}

	class Task implements Runnable {

		final RingBuffer<CharBuf> buffer;
		final CountDownLatch latch;
		final List<DNode> result = new LinkedList<>();
		final Object notifier;

		Task(RingBuffer<CharBuf> buffer, CountDownLatch latch, Object notifier) {
			this.buffer = buffer;
			this.latch = latch;
			this.notifier = notifier;
		}

		public void run() {
			CharBuf t = null;
			try {
				while ((t = buffer.get()) != null) {
					synchronized (notifier) {
						notifier.notifyAll();
					}
					DNode r = parseMatrixLine(t);
					result.add(r);
				}
			} finally {
				latch.countDown();
			}
		}

		DNode parseMatrixLine(CharBuf line) {
			double[] data = parseCSVMatrix(line, olen, idxmap);
			DNode sn = new DNode(line.idx, data);
			CharBuf.POOL.release(line);
			return sn;
		}

	}

	class NDistributor {
		final int mask;
		final RingBuffer<CharBuf>[] buffers;
		int index = 0;
		CountDownLatch latch;
		List<Task> taskList;
		Object lock = new Object();
		int failCnt = 0;

		NDistributor(int thread, int bufSize) {
			List<RingBuffer<CharBuf>> list = new ArrayList<>();
			for (int i = 0; i < thread; i++) {
				list.add(new RingBuffer<>(bufSize));
			}
			buffers = list.toArray(new RingBuffer[0]);
			this.mask = thread - 1;
			taskList = new ArrayList<>(thread);
		}

		public void put(CharBuf res) {
			while (true) {
				int idx = (index + 1) & mask;
				int len = buffers.length * 2;
				boolean ok = false;
				for (; !(ok = buffers[idx].add(res));) {
					idx = (idx + 1) & mask;
					failCnt++;
					if (len-- == 0) {
						break;
					}
				}

				if (ok) {
					index = (idx + 1) & mask;
					break;
				} else {
					synchronized (lock) {
						try {
							lock.wait(1);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}

		public void finish() {
			for (RingBuffer<CharBuf> rb : buffers) {
				rb.finish();
			}
		}

		public void start() {
			latch = new CountDownLatch(mask + 1);
			for (RingBuffer<CharBuf> rb : buffers) {
				Task ctask = new Task(rb, latch, lock);
				taskList.add(ctask);
				Util.executor.execute(ctask);
			}
		}

		public double[][] get() {
			try {
				latch.await();
			} catch (InterruptedException e) {
			}

			int allsize = taskList.stream().mapToInt(l -> l.result.size()).sum();
			List<DNode> list = new ArrayList<>(allsize + 1);
			for (int i = 0; i < taskList.size(); i++) {
				List<DNode> tmp = taskList.get(i).result;
				list.addAll(tmp);
				tmp.clear();
			}

			list.sort(new Comparator<DNode>() {
				public int compare(DNode o1, DNode o2) {
					return o1.idx > o2.idx ? 1 : o1.idx == o2.idx ? 0 : -1;
				}
			});

			int size = list.size();

			double[][] data = new double[size][];
			for (int i = 0; i < size; i++) {
				data[i] = list.get(i).data;
			}

			return data;
		}

	}

}
