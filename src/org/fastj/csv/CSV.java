package org.fastj.csv;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;
import java.util.List;

import static org.fastj.csv.Util.*;

public class CSV {

	/**
	 * 读取CSV文件
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols) throws IOException {
		return readCsv(file, cols, true);
	}

	/**
	 * 读取CSV文件
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param addHeader
	 *            是否在结果中返回列头(Returns the column head in the result?)
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols, boolean addHeader) throws IOException {
		return readCsv(new FileReader(file), cols, addHeader);
	}

	/**
	 * 多线程读取CSV文件
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param readTH
	 *            读取线程数(Number of read-threads)
	 * @param headerLine
	 *            列头所在行数：从0开始(Line-number of table heads: starting from 0)
	 * @param addHeader
	 *            是否在结果中返回列头(Returns the column head in the result?)
	 * @return String[][]
	 */
	public static String[][] readCsv(String file, String[] cols, int readTH, int headerLine, boolean addHeader) throws IOException {
		SimpleMTReader r = new SimpleMTReader(file, readTH, headerLine, cols);
		return r.get(addHeader);
	}

	/**
	 * @param reader
	 *            Reader
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param addHeader
	 *            是否在结果中返回列头(Returns the column head in the result?)
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
	 * 按列头加速读取矩阵 Accelerating read table by column head
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param addHeader
	 *            是否在结果中返回列头(Returns the column head in the result?)
	 * @param readTH
	 *            读取线程数(Number of read-threads)
	 * @param parseTHPerRead
	 *            每个读线程的处理线程数, 必须是2的幂(Number of processing threads per read thread,
	 *            Must be the power of 2)
	 * @param headerLine
	 *            列头所在行数：从0开始(Line-number of table heads: starting from 0)
	 * @return
	 */
	public static String[][] readCsv(String file, String[] cols, boolean addHeader, int readTH, int parseTHPerRead, int headerLine) throws IOException {
		N2NStringReader nr = new N2NStringReader(file, readTH, parseTHPerRead, headerLine, cols);
		return nr.get(addHeader);
	}

	/**
	 * 按列头加速读取矩阵 Accelerating read matrix by column head
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param readTH
	 *            读取线程数(Number of read-threads)
	 * @param parseTHPerRead
	 *            每个读线程的处理线程数, 必须是2的幂(Number of processing threads per read thread,
	 *            Must be the power of 2)
	 * @param headerLine
	 *            列头所在行数：从0开始(Line-number of table heads: starting from 0)
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols, int readTH, int parseTHPerRead, int headerLine) throws IOException {
		N2NMatrixReader nr = new N2NMatrixReader(file, readTH, parseTHPerRead, headerLine, cols);
		return nr.get();
	}

	/**
	 * 按列读取矩阵 Read matrix by column
	 * 
	 * @param reader
	 *            Reader
	 * @param cols
	 *            需要读取的列头(Columns to read)
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
	 * 按列读取矩阵 Read matrix by column
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols) throws IOException {
		return readMatrix(new FileReader(file), cols);
	}

	/**
	 * 多线程读取矩阵
	 * 
	 * @param file
	 *            仅支持本地文件(Local file only)
	 * @param cols
	 *            需要读取的列头(Columns to read)
	 * @param readTH
	 *            读取线程数(Number of read-threads)
	 * @param headerLine
	 *            列头所在行数：从0开始(Line-number of table heads: starting from 0)
	 * @return double[][]
	 */
	public static double[][] readMatrix(String file, String[] cols, int readTH, int headerLine) throws IOException {
		SimpleMTReader r = new SimpleMTReader(file, readTH, headerLine, cols);
		return r.get();
	}

}
