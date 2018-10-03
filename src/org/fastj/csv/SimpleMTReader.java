package org.fastj.csv;

import static org.fastj.csv.Util.createIdxMap;
import static org.fastj.csv.Util.createVidx;
import static org.fastj.csv.Util.parseCSVLine;
import static org.fastj.csv.Util.parseCSVMatrix;
import static org.fastj.csv.Util.parseSimple;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class SimpleMTReader {

	ChannelReader[] readers;
	String[] header;
	int olen;
	int[] vidx;
	int[] idxmap;

	SimpleMTReader(String file, int rsize, int headline, String[] rcols) throws IOException {

		long fsize = Files.size(new File(file).toPath());
		long blockSize = fsize / rsize + 1;

		readers = new ChannelReader[rsize];

		for (int i = 0; i < rsize; i++) {
			readers[i] = new ChannelReader(file, blockSize * i, blockSize, i != 0 ? 0 : -1);
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

		DNode[] tmpRlts = new DNode[readers.length];
		for (int i = 0; i < readers.length; i++) {
			final int pc = i;
			Util.executor.execute(() -> {
				try {
					double[][] data = readMatrix(readers[pc]);
					tmpRlts[pc] = new DNode(data);
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

		for (DNode pn : tmpRlts) {
			total += pn.data.length;
		}

		double[][] data = new double[total][];

		int pc = 0;
		for (DNode pn : tmpRlts) {
			for (double[] row : pn.data) {
				data[pc++] = row;
			}
		}

		return data;
	}

	String[][] get(boolean withHeader) {
		final CountDownLatch cdl = new CountDownLatch(readers.length);

		PNode[] tmpRlts = new PNode[readers.length];
		for (int i = 0; i < readers.length; i++) {
			final int pc = i;
			Util.executor.execute(() -> {
				try {
					String[][] data = readCsv(readers[pc]);
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

	String[][] readCsv(ChannelReader reader) {
		CharBuf buf = CharBuf.POOL.get();
		CharBuf field = CharBuf.POOL.get();
		List<String[]> result = new LinkedList<>();
		try (ChannelReader r = reader) {
			CharBuf line = buf;
			while ((line = r.readLine(line)) != null) {
				String[] row = parseCSVLine(line, field, olen, idxmap);
				result.add(row);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CharBuf.POOL.release(buf);
			CharBuf.POOL.release(field);
		}

		String[][] data = result.toArray(new String[result.size()][]);
		return data;
	}

	double[][] readMatrix(ChannelReader reader) {
		CharBuf buf = CharBuf.POOL.get();
		List<double[]> result = new LinkedList<>();
		try (ChannelReader r = reader) {
			CharBuf line = buf;
			while ((line = r.readLine(line)) != null) {
				double[] row = parseCSVMatrix(line, olen, idxmap);
				result.add(row);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			CharBuf.POOL.release(buf);
		}

		double[][] data = result.toArray(new double[result.size()][]);
		return data;
	}

	class PNode {
		String[][] data;

		PNode(String[][] data) {
			this.data = data;
		}
	}

	class DNode {
		double[][] data;

		DNode(double[][] data) {
			this.data = data;
		}
	}

}
