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

final class N2NStringReader {

	ChannelReader[] readers;
	Distributor[] processors;
	String[] header;
	int olen;
	int[] vidx;
	int[] idxmap;

	N2NStringReader(String file, int rsize, int psizePerReader, int headline, String[] rcols) throws IOException {

		long fsize = Files.size(new File(file).toPath());
		long blockSize = fsize / rsize + 1;

		readers = new ChannelReader[rsize];
		processors = new Distributor[rsize];

		for (int i = 0; i < rsize; i++) {
			readers[i] = new ChannelReader(file, blockSize * i, blockSize, i != 0 ? 0 : -1);
			processors[i] = new Distributor(psizePerReader, 32);
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

	String[][] get(boolean withHeader) {
		final CountDownLatch cdl = new CountDownLatch(readers.length);

		PNode[] tmpRlts = new PNode[readers.length];
		for (int i = 0; i < readers.length; i++) {
			final int pc = i;
			Util.executor.execute(() -> {
				try {
					String[][] data = readCsv(readers[pc], processors[pc]);
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

		int total = withHeader ? 1 : 0;

		for (PNode pn : tmpRlts) {
			total += pn.data.length;
		}

		String[][] data = new String[total][];

		int pc = 0;
		if (withHeader) {
			data[0] = header;
			pc = 1;
		}
		for (PNode pn : tmpRlts) {
			for (String[] row : pn.data) {
				data[pc++] = row;
			}
		}

		return data;
	}

	public String[] getHeader() {
		return header;
	}

	String[][] readCsv(ChannelReader reader, Distributor processor) {
		processor.start();
		CharBuf buf = CharBuf.POOL.get();
		String[][] data = new String[0][0];

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
		String[][] data;

		PNode(String[][] data) {
			this.data = data;
		}
	}

	class StrNode {
		int idx;
		String[] data;

		StrNode(int idx, String[] data) {
			this.idx = idx;
			this.data = data;
		}
	}

	class Task implements Runnable {

		final RingBuffer<CharBuf> buffer;
		final CountDownLatch latch;
		final List<StrNode> result = new LinkedList<>();
		CharBuf field = CharBuf.POOL.get();
		Object notifier;

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
					StrNode r = parseMatrixLine(t);
					result.add(r);
				}
			} finally {
				latch.countDown();
				CharBuf.POOL.release(field);
			}
		}

		StrNode parseMatrixLine(CharBuf line) {
			String[] data = parseCSVLine(line, field, olen, idxmap);
			StrNode sn = new StrNode(line.idx, data);
			CharBuf.POOL.release(line);
			return sn;
		}

	}

	class Distributor {
		int index = 0;
		final int mask;
		final RingBuffer<CharBuf>[] buffers;
		CountDownLatch latch;
		List<Task> taskList;
		Object lock = new Object();
		int failCnt = 0;

		Distributor(int thread, int bufSize) {
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

		public String[][] get() {
			try {
				latch.await();
			} catch (InterruptedException e) {
			}

			int allsize = taskList.stream().mapToInt(l -> l.result.size()).sum();
			List<StrNode> list = new ArrayList<>(allsize + 1);
			for (int i = 0; i < taskList.size(); i++) {
				List<StrNode> tmp = taskList.get(i).result;
				list.addAll(tmp);
				tmp.clear();
			}

			list.sort(new Comparator<StrNode>() {
				public int compare(StrNode o1, StrNode o2) {
					return o1.idx > o2.idx ? 1 : o1.idx == o2.idx ? 0 : -1;
				}
			});

			int size = list.size();
			String[][] data = new String[size][];
			for (int i = 0; i < size; i++) {
				data[i] = list.get(i).data;
			}

			return data;
		}

	}

}
