package org.fastj.csv;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

import static org.fastj.csv.Util.*;

public class CSV {

	/**
	 * Read CSV file
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols) throws IOException {
		return readCsv(file, cols, true);
	}

	/**
	 * Read CSV file
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read
	 * @param addHeader Whether to return the column header in the result(Returns the column head in the result?)
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols, boolean addHeader) throws IOException {
		return readCsv(new FileReader(file), cols, addHeader);
	}

	/**
	 * Multi-threaded read CSV file
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read
	 * @param readTH Number of read-threads
	 * @param headerLine Number of rows in the column header：Starting from 0(Line-number of table heads)
	 * @param addHeader Whether to return the column header in the result(Returns the column head in the result?)
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols, int readTH, int headerLine, boolean addHeader) throws IOException {
		SimpleMTReader r = new SimpleMTReader(file, readTH, headerLine, cols);
		return r.get(addHeader);
	}

	/**
	 * @param reader Reader
	 * @param cols Column headers that need to be read
	 * @param addHeader Whether to return the column header in the result(Returns the column head in the result?)
	 * @return String[][]
	 */
	public static String[][] readCsv(Reader reader, String[] cols, boolean addHeader) throws IOException {
		List<String[]> list = new LinkedList<>();

		CharBuf buf = CharBuf.POOL.get();
		CharBuf fieldBuf = CharBuf.POOL.get();
		CharBuf line = buf;
		String[][] data = null;

		try (FastReader r = new FastReader(reader)) {
			r.readLine(line);
			String[] headers = parseSimple(line);
			if (addHeader) {
				list.add(cols != null ? cols : headers);
			}
			int[] vidx = createVidx(headers, cols);
			int[] idxmap = createIdxMap(headers.length, vidx);

			while ((line = r.readLine(line)) != null) {
				list.add(parseCSVLine(line, fieldBuf, vidx.length, idxmap));
			}

			data = list.toArray(new String[list.size()][]);
		} finally {
			CharBuf.POOL.release(buf);
			CharBuf.POOL.release(fieldBuf);
		}

		return data;
	}

	/**
	 * Accelerate reading matrix by column head Accelerating read table by column head
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read(
	 * @param addHeader Whether to return the column header in the result(Returns the column head in the result?)
	 * @param readTH Number of threads read(Number of read-threads)
	 * @param parseTHPerRead Number of processing threads per read-threads, Must be a power of 2
	 * @param headerLine Number of rows in the column header：Starting from 0(Line-number of table heads)
	 * @return
	 */
	public static String[][] readCsv(String file, String[] cols, boolean addHeader, int readTH, int parseTHPerRead, int headerLine) throws IOException {
		N2NStringReader nr = new N2NStringReader(file, readTH, parseTHPerRead, headerLine, cols);
		return nr.get(addHeader);
	}

	/**
	 * Accelerate reading matrix by column head Accelerating read matrix by column head
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read
	 * @param readTH Number of read-threads
	 * @param parseTHPerRead Number of processing threads per read-threads, Must be a power of 2
	 * @param headerLine Number of rows in the column header：Starting from 0(Line-number of table heads)
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols, int readTH, int parseTHPerRead, int headerLine) throws IOException {
		N2NMatrixReader nr = new N2NMatrixReader(file, readTH, parseTHPerRead, headerLine, cols);
		return nr.get();
	}

	/**
	 * Read matrix by column Read matrix by column
	 * 
	 * @param reader Reader
	 * @param cols Column headers that need to be read
	 * @return double[][]
	 */
	public static double[][] readMatrix(Reader reader, String[] cols) throws IOException {

		List<double[]> list = new LinkedList<>();

		double[][] rlt = null;

		try (FastReader r = new FastReader(reader)) {
			CharBuf line = null;
			CharBuf buf = r.readLine(null);
			String[] headers = parseSimple(buf);
			int[] vidx = createVidx(headers, cols);
			int[] idxmap = createIdxMap(headers.length, vidx);
			while ((line = r.readLine(buf)) != null) {
				double[] row = parseCSVMatrix(line, vidx.length, idxmap);
				list.add(row);
			}

			rlt = list.toArray(new double[list.size()][]);
		} finally {
			// nothing
		}

		return rlt;
	}

	/**
	 * Read matrix by column Read matrix by column
	 * 
	 * @param file Only local files are supported
	 * @param cols Column headers that need to be read
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols) throws IOException {
		return readMatrix(new FileReader(file), cols);
	}

	/**
	 * Multi-threaded read matrix
	 * 
	 * @param file Only local files are supported
	 * @param cols  Column headers that need to be read
	 * @param readTH Number of read-threads
	 * @param headerLine Number of rows in the column header：Starting from 0(Line-number of table heads)
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols, int readTH, int headerLine) throws IOException {
		SimpleMTReader r = new SimpleMTReader(file, readTH, headerLine, cols);
		return r.get();
	}

}
