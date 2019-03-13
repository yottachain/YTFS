#!/bin/bash

# wrapper call to ./playground --config config-file.json --test stress
RESULT=./bench_result.log
HOME_DIR=../playground
PLAY_CMD=$HOME_DIR/playground
#PLAY_CMD=echo

reviseConfig() {
    CONF=$1.json
    cp ytfs.config.template $CONF
    
    for ((i = 2; i <= $#; i += 2 )); do
	KEY=$i
	NAME=${!KEY}
	VALUE=$((i+1))
	PROP=${!VALUE}
	sed -i -e "s#$NAME.*#$NAME\": $PROP,#g" $CONF
    done
}

runConfig() {
    reviseConfig $@
    YTFS_HOME=/tmp/$1
    TEST_MODE=stress
    mkdir $YTFS_HOME
#    echo $PLAY_CMD --config $1.json --test format --home $YTFS_HOME
#    $PLAY_CMD --config $1.json --format --home $YTFS_HOME

    STARTTIME=$(date +%s)
    echo $PLAY_CMD --config $1.json --test write --home $YTFS_HOME
    $PLAY_CMD --config $1.json --test write --home $YTFS_HOME
    ENDTIME=$(date +%s)
    echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete $1 write..."
    echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete $1 write..." >> $RESULT

    STARTTIME=$(date +%s)
    echo $PLAY_CMD --config $1.json --test read --home $YTFS_HOME
    $PLAY_CMD --config $1.json --test read --home $YTFS_HOME
    ENDTIME=$(date +%s)
    echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete $1 read..."
    echo "It takes $(($ENDTIME - $STARTTIME)) seconds to complete $1 read..." >> $RESULT

#    echo $PLAY_CMD --config $1.json --test stress --home $YTFS_HOME
#    $PLAY_CMD --config $1.json --test stress --home $YTFS_HOME
    rm -rf $YTFS_HOME
    rm -rf /tmp/yotta-test*
}

rm -rf $RESULT
touch $RESULT

run8kDataBench() {
    #8K data bench
    runConfig "config-16t-2x2g-16kRange-8kData"  C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((16<<10))
    runConfig "config-16t-2x2g-64kRange-8kData"  C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((64<<10))
    runConfig "config-16t-2x2g-256kRange-8kData" C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((256<<10))
    runConfig "config-16t-2x2g-1mRange-8kData"   C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((1024<<10))
    runConfig "config-16t-2x2g-4mRange-8kData"   C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((4096<<10))

    runConfig "config-64t-2x2g-16kRange-8kData"  C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((16<<10))
    runConfig "config-64t-2x2g-64kRange-8kData"  C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((64<<10))
    runConfig "config-64t-2x2g-256kRange-8kData" C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((256<<10))
    runConfig "config-64t-2x2g-1mRange-8kData"   C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((1024<<10))
    runConfig "config-64t-2x2g-4mRange-8kData"   C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((4096<<10))

    #8k data sync bench
    runConfig "config-sync-16t-2x2g-16kRange-8kData"  writesync true C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((16<<10))
    runConfig "config-sync-16t-2x2g-64kRange-8kData"  writesync true C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((64<<10))
    runConfig "config-sync-16t-2x2g-256kRange-8kData" writesync true C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((256<<10))
    runConfig "config-sync-16t-2x2g-1mRange-8kData"   writesync true C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((1024<<10))
    runConfig "config-sync-16t-2x2g-4mRange-8kData"   writesync true C $((16<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((4096<<10))

    runConfig "config-sync-64t-2x2g-16kRange-8kData"  writesync true C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((16<<10))
    runConfig "config-sync-64t-2x2g-64kRange-8kData"  writesync true C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((64<<10))
    runConfig "config-sync-64t-2x2g-256kRange-8kData" writesync true C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((256<<10))
    runConfig "config-sync-64t-2x2g-1mRange-8kData"   writesync true C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((1024<<10))
    runConfig "config-sync-64t-2x2g-4mRange-8kData"   writesync true C $((64<<40)) storageSize $((2<<30)) dataBlockSize $((8<<10)) D $((8<<10)) N $((4096<<10))
}

run32kDataBench() {
    #32K data bench
    runConfig "config-16t-2x2g-16kRange-32kData"  C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((16<<10))
    runConfig "config-16t-2x2g-64kRange-32kData"  C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((64<<10))
    runConfig "config-16t-2x2g-256kRange-32kData" C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((256<<10))
    runConfig "config-16t-2x2g-1mRange-32kData"   C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((1024<<10))
    runConfig "config-16t-2x2g-4mRange-32kData"   C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((4096<<10))

    runConfig "config-64t-2x2g-16kRange-32kData"  C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((16<<10))
    runConfig "config-64t-2x2g-64kRange-32kData"  C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((64<<10))
    runConfig "config-64t-2x2g-256kRange-32kData" C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((256<<10))
    runConfig "config-64t-2x2g-1mRange-32kData"   C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((1024<<10))
    runConfig "config-64t-2x2g-4mRange-32kData"   C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((4096<<10))

    #8k data sync bench
    runConfig "config-sync-16t-2x2g-16kRange-32kData"  writesync true C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((16<<10))
    runConfig "config-sync-16t-2x2g-64kRange-32kData"  writesync true C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((64<<10))
    runConfig "config-sync-16t-2x2g-256kRange-32kData" writesync true C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((256<<10))
    runConfig "config-sync-16t-2x2g-1mRange-32kData"   writesync true C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((1024<<10))
    runConfig "config-sync-16t-2x2g-4mRange-32kData"   writesync true C $((16<<40)) storageSize $((2<<30)) D $((32<<10)) N $((4096<<10))

    runConfig "config-sync-64t-2x2g-16kRange-32kData"  writesync true C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((16<<10))
    runConfig "config-sync-64t-2x2g-64kRange-32kData"  writesync true C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((64<<10))
    runConfig "config-sync-64t-2x2g-256kRange-32kData" writesync true C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((256<<10))
    runConfig "config-sync-64t-2x2g-1mRange-32kData"   writesync true C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((1024<<10))
    runConfig "config-sync-64t-2x2g-4mRange-32kData"   writesync true C $((64<<40)) storageSize $((2<<30)) D $((32<<10)) N $((4096<<10))
}

run8kDataBench
run32kDataBench
