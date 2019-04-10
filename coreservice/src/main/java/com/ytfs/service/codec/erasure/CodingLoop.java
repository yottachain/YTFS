package com.ytfs.service.codec.erasure;

public interface CodingLoop {

    CodingLoop[] ALL_CODING_LOOPS
            = new CodingLoop[]{
                new ByteInputOutputExpCodingLoop(),
                new ByteInputOutputTableCodingLoop(),
                new ByteOutputInputExpCodingLoop(),
                new ByteOutputInputTableCodingLoop(),
                new InputByteOutputExpCodingLoop(),
                new InputByteOutputTableCodingLoop(),
                new InputOutputByteExpCodingLoop(),
                new InputOutputByteTableCodingLoop(),
                new OutputByteInputExpCodingLoop(),
                new OutputByteInputTableCodingLoop(),
                new OutputInputByteExpCodingLoop(),
                new OutputInputByteTableCodingLoop(),};
 
    void codeSomeShards(final byte[][] matrixRows,
            final byte[][] inputs,
            final int inputCount,
            final byte[][] outputs,
            final int outputCount,
            final int offset,
            final int byteCount);

    
    boolean checkSomeShards(final byte[][] matrixRows,
            final byte[][] inputs,
            final int inputCount,
            final byte[][] toCheck,
            final int checkCount,
            final int offset,
            final int byteCount,
            final byte[] tempBuffer);
}
