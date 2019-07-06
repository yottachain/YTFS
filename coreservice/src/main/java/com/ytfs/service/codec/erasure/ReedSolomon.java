package com.ytfs.service.codec.erasure;

public class ReedSolomon {

    private final int dataShardCount;
    private final int parityShardCount;
    private final int totalShardCount;
    private final Matrix matrix;
    private final CodingLoop codingLoop;
    private final byte[][] parityRows;

    public static ReedSolomon create(int dataShardCount, int parityShardCount) {
        return new ReedSolomon(dataShardCount, parityShardCount, new InputOutputByteTableCodingLoop());
    }

    public ReedSolomon(int dataShardCount, int parityShardCount, CodingLoop codingLoop) {
        if (256 < dataShardCount + parityShardCount) {
            throw new IllegalArgumentException("too many shards - max is 256");
        }
        this.dataShardCount = dataShardCount;
        this.parityShardCount = parityShardCount;
        this.codingLoop = codingLoop;
        this.totalShardCount = dataShardCount + parityShardCount;
        matrix = buildMatrix(dataShardCount, this.totalShardCount);
        parityRows = new byte[parityShardCount][];
        for (int i = 0; i < parityShardCount; i++) {
            parityRows[i] = matrix.getRow(dataShardCount + i);
        }
    }

    public int getDataShardCount() {
        return dataShardCount;
    }

    public int getParityShardCount() {
        return parityShardCount;
    }

    public int getTotalShardCount() {
        return totalShardCount;
    }

    public void encodeParity(byte[][] shards, int offset, int byteCount) {
        byte[][] outputs = new byte[parityShardCount][];
        System.arraycopy(shards, dataShardCount, outputs, 0, parityShardCount);
        codingLoop.codeSomeShards(
                parityRows,
                shards, dataShardCount,
                outputs, parityShardCount,
                offset, byteCount);
    }

    public boolean isParityCorrect(byte[][] shards, int firstByte, int byteCount) {
        checkBuffersAndSizes(shards, firstByte, byteCount);
        byte[][] toCheck = new byte[parityShardCount][];
        System.arraycopy(shards, dataShardCount, toCheck, 0, parityShardCount);
        return codingLoop.checkSomeShards(
                parityRows,
                shards, dataShardCount,
                toCheck, parityShardCount,
                firstByte, byteCount,
                null);
    }

    public boolean isParityCorrect(byte[][] shards, int firstByte, int byteCount, byte[] tempBuffer) {
        checkBuffersAndSizes(shards, firstByte, byteCount);
        if (tempBuffer.length < firstByte + byteCount) {
            throw new IllegalArgumentException("tempBuffer is not big enough");
        }
        byte[][] toCheck = new byte[parityShardCount][];
        System.arraycopy(shards, dataShardCount, toCheck, 0, parityShardCount);
        return codingLoop.checkSomeShards(
                parityRows,
                shards, dataShardCount,
                toCheck, parityShardCount,
                firstByte, byteCount,
                tempBuffer);
    }

    public void decodeMissing(byte[][] shards,
            boolean[] shardPresent,
            final int offset,
            final int byteCount) {
        Matrix subMatrix = new Matrix(dataShardCount, dataShardCount);
        byte[][] subShards = new byte[dataShardCount][];
        {
            int subMatrixRow = 0;
            for (int matrixRow = 0; matrixRow < totalShardCount && subMatrixRow < dataShardCount; matrixRow++) {
                if (shardPresent[matrixRow]) {
                    for (int c = 0; c < dataShardCount; c++) {
                        subMatrix.set(subMatrixRow, c, matrix.get(matrixRow, c));
                    }
                    subShards[subMatrixRow] = shards[matrixRow];
                    subMatrixRow += 1;
                }
            }
        }
        Matrix dataDecodeMatrix = subMatrix.invert();
        byte[][] outputs = new byte[parityShardCount][];
        byte[][] matrixRows = new byte[parityShardCount][];
        int outputCount = 0;
        for (int iShard = 0; iShard < dataShardCount; iShard++) {
            if (!shardPresent[iShard]) {
                outputs[outputCount] = shards[iShard];
                matrixRows[outputCount] = dataDecodeMatrix.getRow(iShard);
                outputCount += 1;
            }
        }
        codingLoop.codeSomeShards(
                matrixRows,
                subShards, dataShardCount,
                outputs, outputCount,
                offset, byteCount);
        outputCount = 0;
        for (int iShard = dataShardCount; iShard < totalShardCount; iShard++) {
            if (!shardPresent[iShard]) {
                outputs[outputCount] = shards[iShard];
                matrixRows[outputCount] = parityRows[iShard - dataShardCount];
                outputCount += 1;
            }
        }
        codingLoop.codeSomeShards(
                matrixRows,
                shards, dataShardCount,
                outputs, outputCount,
                offset, byteCount);
    }

    private void checkBuffersAndSizes(byte[][] shards, int offset, int byteCount) {
        if (shards.length != totalShardCount) {
            throw new IllegalArgumentException("wrong number of shards: " + shards.length);
        }
        int shardLength = shards[0].length;
        for (int i = 1; i < shards.length; i++) {
            if (shards[i].length != shardLength) {
                throw new IllegalArgumentException("Shards are different sizes");
            }
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset is negative: " + offset);
        }
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount is negative: " + byteCount);
        }
        if (shardLength < offset + byteCount) {
            throw new IllegalArgumentException("buffers to small: " + byteCount + offset);
        }
    }

    private static Matrix buildMatrix(int dataShards, int totalShards) {
        Matrix vandermonde = vandermonde(totalShards, dataShards);
        Matrix top = vandermonde.submatrix(0, 0, dataShards, dataShards);
        return vandermonde.times(top.invert());
    }

    private static Matrix vandermonde(int rows, int cols) {
        Matrix result = new Matrix(rows, cols);
        for (int r = 0; r < rows; r++) {
            for (int c = 0; c < cols; c++) {
                result.set(r, c, Galois.exp((byte) r, c));
            }
        }
        return result;
    }
}
