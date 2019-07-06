package com.ytfs.service.codec.erasure;

public abstract class CodingLoopBase implements CodingLoop {

    @Override
    public boolean checkSomeShards(
            byte[][] matrixRows,
            byte[][] inputs, int inputCount,
            byte[][] toCheck, int checkCount,
            int offset, int byteCount,
            byte[] tempBuffer) {
        byte[][] table = Galois.MULTIPLICATION_TABLE;
        for (int iByte = offset; iByte < offset + byteCount; iByte++) {
            for (int iOutput = 0; iOutput < checkCount; iOutput++) {
                byte[] matrixRow = matrixRows[iOutput];
                int value = 0;
                for (int iInput = 0; iInput < inputCount; iInput++) {
                    value ^= table[matrixRow[iInput] & 0xFF][inputs[iInput][iByte] & 0xFF];
                }
                if (toCheck[iOutput][iByte] != (byte) value) {
                    return false;
                }
            }
        }
        return true;
    }
}
