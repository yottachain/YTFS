package com.ytfs.service.codec.erasure;

import java.util.Arrays;

public class Matrix {

    private final int rows;
    private final int columns;
    private final byte[][] data;

    public Matrix(int initRows, int initColumns) {
        rows = initRows;
        columns = initColumns;
        data = new byte[rows][];
        for (int r = 0; r < rows; r++) {
            data[r] = new byte[columns];
        }
    }

    public Matrix(byte[][] initData) {
        rows = initData.length;
        columns = initData[0].length;
        data = new byte[rows][];
        for (int r = 0; r < rows; r++) {
            if (initData[r].length != columns) {
                throw new IllegalArgumentException("Not all rows have the same number of columns");
            }
            data[r] = new byte[columns];
            System.arraycopy(initData[r], 0, data[r], 0, columns);
        }
    }

    public static Matrix identity(int size) {
        Matrix result = new Matrix(size, size);
        for (int i = 0; i < size; i++) {
            result.set(i, i, (byte) 1);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append('[');
        for (int r = 0; r < rows; r++) {
            if (r != 0) {
                result.append(", ");
            }
            result.append('[');
            for (int c = 0; c < columns; c++) {
                if (c != 0) {
                    result.append(", ");
                }
                result.append(data[r][c] & 0xFF);
            }
            result.append(']');
        }
        result.append(']');
        return result.toString();
    }

    public String toBigString() {
        StringBuilder result = new StringBuilder();
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < columns; c++) {
                int value = get(r, c);
                if (value < 0) {
                    value += 256;
                }
                result.append(String.format("%02x ", value));
            }
            result.append("\n");
        }
        return result.toString();
    }

    public int getColumns() {
        return columns;
    }

    public int getRows() {
        return rows;
    }

    public byte get(int r, int c) {
        if (r < 0 || rows <= r) {
            throw new IllegalArgumentException("Row index out of range: " + r);
        }
        if (c < 0 || columns <= c) {
            throw new IllegalArgumentException("Column index out of range: " + c);
        }
        return data[r][c];
    }

    public void set(int r, int c, byte value) {
        if (r < 0 || rows <= r) {
            throw new IllegalArgumentException("Row index out of range: " + r);
        }
        if (c < 0 || columns <= c) {
            throw new IllegalArgumentException("Column index out of range: " + c);
        }
        data[r][c] = value;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Matrix)) {
            return false;
        }
        for (int r = 0; r < rows; r++) {
            if (!Arrays.equals(data[r], ((Matrix) other).data[r])) {
                return false;
            }
        }
        return true;
    }

    public Matrix times(Matrix right) {
        if (getColumns() != right.getRows()) {
            throw new IllegalArgumentException(
                    "Columns on left (" + getColumns() + ") "
                    + "is different than rows on right (" + right.getRows() + ")");
        }
        Matrix result = new Matrix(getRows(), right.getColumns());
        for (int r = 0; r < getRows(); r++) {
            for (int c = 0; c < right.getColumns(); c++) {
                byte value = 0;
                for (int i = 0; i < getColumns(); i++) {
                    value ^= Galois.multiply(get(r, i), right.get(i, c));
                }
                result.set(r, c, value);
            }
        }
        return result;
    }

    public Matrix augment(Matrix right) {
        if (rows != right.rows) {
            throw new IllegalArgumentException("Matrices don't have the same number of rows");
        }
        Matrix result = new Matrix(rows, columns + right.columns);
        for (int r = 0; r < rows; r++) {
            System.arraycopy(data[r], 0, result.data[r], 0, columns);
            System.arraycopy(right.data[r], 0, result.data[r], columns, right.columns);
        }
        return result;
    }

    public Matrix submatrix(int rmin, int cmin, int rmax, int cmax) {
        Matrix result = new Matrix(rmax - rmin, cmax - cmin);
        for (int r = rmin; r < rmax; r++) {
            for (int c = cmin; c < cmax; c++) {
                result.data[r - rmin][c - cmin] = data[r][c];
            }
        }
        return result;
    }

    public byte[] getRow(int row) {
        byte[] result = new byte[columns];
        for (int c = 0; c < columns; c++) {
            result[c] = get(row, c);
        }
        return result;
    }

    public void swapRows(int r1, int r2) {
        if (r1 < 0 || rows <= r1 || r2 < 0 || rows <= r2) {
            throw new IllegalArgumentException("Row index out of range");
        }
        byte[] tmp = data[r1];
        data[r1] = data[r2];
        data[r2] = tmp;
    }

    public Matrix invert() {
        if (rows != columns) {
            throw new IllegalArgumentException("Only square matrices can be inverted");
        }
        Matrix work = augment(identity(rows));
        work.gaussianElimination();
        return work.submatrix(0, rows, columns, columns * 2);
    }

    private void gaussianElimination() {
        for (int r = 0; r < rows; r++) {
            if (data[r][r] == (byte) 0) {
                for (int rowBelow = r + 1; rowBelow < rows; rowBelow++) {
                    if (data[rowBelow][r] != 0) {
                        swapRows(r, rowBelow);
                        break;
                    }
                }
            }
            if (data[r][r] == (byte) 0) {
                throw new IllegalArgumentException("Matrix is singular");
            }
            if (data[r][r] != (byte) 1) {
                byte scale = Galois.divide((byte) 1, data[r][r]);
                for (int c = 0; c < columns; c++) {
                    data[r][c] = Galois.multiply(data[r][c], scale);
                }
            }
            for (int rowBelow = r + 1; rowBelow < rows; rowBelow++) {
                if (data[rowBelow][r] != (byte) 0) {
                    byte scale = data[rowBelow][r];
                    for (int c = 0; c < columns; c++) {
                        data[rowBelow][c] ^= Galois.multiply(scale, data[r][c]);
                    }
                }
            }
        }
        for (int d = 0; d < rows; d++) {
            for (int rowAbove = 0; rowAbove < d; rowAbove++) {
                if (data[rowAbove][d] != (byte) 0) {
                    byte scale = data[rowAbove][d];
                    for (int c = 0; c < columns; c++) {
                        data[rowAbove][c] ^= Galois.multiply(scale, data[d][c]);
                    }
                }
            }
        }
    }

}
