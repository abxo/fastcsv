package org.fastj.csv;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

public class Util {

	public static final ExecutorService executor = Executors.newCachedThreadPool(new ThreadFactory() {
		AtomicLong idx = new AtomicLong();

		public Thread newThread(Runnable r) {
			Thread t = new Thread(r);
			t.setDaemon(true);
			t.setName("csv-parse-" + idx.incrementAndGet());
			return t;
		}
	});

	public static String[] parseCSVLine(CharBuf line, CharBuf fieldBuf, int len, int[] cols) {

		String[] rlt = new String[len];
		char[] buf = line.value;
		int buflen = line.count;
		int fcnt = -1;
		int setc = 0;
		int nc = 0;

		for (; nc < buflen && setc < len;) {
			byte bit = 0x00;
			char c = 0;
			int i = nc;
			for (; i < buflen; i++) {
				c = buf[i];
				if (c > ',') {
					continue;
				}
				if (c == '"') {
					bit ^= 0x01;
					continue;
				}
				if (c == ',') {
					if (bit == 0) {
						break;
					}
				}
			}

			fcnt++;
			int idx = cols[fcnt];
			if (idx >= 0) {
				rlt[idx] = getCSVFieldString(fieldBuf, buf, nc, i - 1);
				setc++;
			}

			nc = ++i;
		}

		if (setc == len - 1) {
			int idx = cols[++fcnt];
			rlt[idx] = "";
		}

		return rlt;
	}

	public static double[] parseCSVMatrix(CharBuf line, int len, int[] cols) {
		double[] rlt = new double[len];
		char[] buf = line.value;
		int buflen = line.count;
		int fcnt = -1;
		int setc = 0;
		int nc = 0;

		for (; nc < buflen && setc < len;) {
			byte bit = 0x00;
			char c = 0;
			int i = nc;
			for (; i < buflen; i++) {
				c = buf[i];
				if (c > ',') {
					continue;
				}
				if (c == '"') {
					bit ^= 0x01;
					continue;
				}
				if (c == ',') {
					if (bit == 0) {
						break;
					}
				}
			}

			fcnt++;
			int idx = cols[fcnt];
			if (idx >= 0) {
				rlt[idx] = Double.parseDouble(new String(line.value, nc, i - nc));
				setc++;
			}

			nc = ++i;
		}

		if (setc == len - 1) {
			int idx = cols[++fcnt];
			rlt[idx] = 0.0;
		}

		return rlt;
	}

	private static String getCSVFieldString(CharBuf field, char[] value, int start, int end) {

		if (value[start] != '"') {
			return new String(value, start, end - start + 1);
		} else {
			start++;
			end--;
		}

		field.reset();
		char c = 0;
		int copyStart = start;
		int i = start;
		for (; i <= end;) {
			c = value[i];
			if (c != '"') {
				i++;
			} else {
				field.append(value, copyStart, i - copyStart + 1);
				i += 2;
				copyStart = i;
			}
		}

		if (i > copyStart) {
			field.append(value, copyStart, i - copyStart);
		}

		return field.toStringAndReset();
	}

	public static String[] parseSimple(CharBuf line) {
		List<String> l = new LinkedList<>();
		int copyStart = 0;
		int curSplit = 0;
		int end = line.count;
		char[] buf = line.value;

		int pos = 0;
		while (copyStart < end) {
			int i = copyStart;
			for (; i < end && buf[i] != ','; i++)
				;
			curSplit = i;
			if (curSplit == copyStart) {
				l.add("");
			} else {
				l.add(new String(buf, copyStart, curSplit - copyStart));
			}
			pos++;
			copyStart = curSplit + 1;
		}

		return l.toArray(new String[pos]);
	}

	static int[] createVidxExclude(String[] headers, String[] exCols) {

		List<String> cols = new LinkedList<>();
		Collections.addAll(cols, headers);

		for (String h : exCols) {
			cols.remove(h);
		}

		return createVidx(headers, cols.toArray(new String[0]));
	}

	static int[] createVidx(String[] headers, String[] cols) {
		Objects.requireNonNull(headers, "Headers must not null");
		if (cols == null) {
			int[] vidx = new int[headers.length];
			for (int i = 0; i < vidx.length; i++) {
				vidx[i] = i;
			}
			return vidx;
		}

		int[] vidx = new int[cols.length];
		int m = cols.length;
		int n = headers.length;
		for (int i = 0; i < m; i++) {
			vidx[i] = -1;
			for (int j = 0; j < n; j++) {
				if (cols[i].equals(headers[j])) {
					vidx[i] = j;
					break;
				}
			}
			if (vidx[i] < 0) {
				throw new IllegalArgumentException(cols[i] + " not found");
			}
		}
		return vidx;
	}

	static int[] createIdxMap(int length, int[] vidx) {
		int[] idxmap = new int[length];
		for (int i = 0; i < length; i++) {
			idxmap[i] = -1;
		}
		for (int i = 0; i < vidx.length; i++) {
			idxmap[vidx[i]] = i;
		}

		return idxmap;
	}

}
