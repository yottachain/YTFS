#!/bin/bash

# wrapper call to ./playground --config config-file.json --test stress
HOME_DIR=../playground
#PLAY_CMD=$HOME_DIR/playground
PLAY_CMD=echo

reviseConfig() {
    cp YTFS-file.json.template $1
    
    for ((i = 2; i <= $#; i += 2 )); do
	KEY=$i
	NAME=${!KEY}
	VALUE=$((i+1))
	PROP=${!VALUE}
	sed -i -e "s#$NAME.*#$NAME\": $PROP,#g" $1
    done
}

FILE="config-1g-32k.json"
reviseConfig $FILE T $((1<<30)) storage \"/tmp/yotta-disk-storage.test\"
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-1g-32k-sync.json"
reviseConfig $FILE T $((1<<30)) storage \"/tmp/yotta-disk-storage.test\" writesync true
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-4g-32k.json"
reviseConfig $FILE T $((1<<32)) storage \"/tmp/yotta-disk-storage.test\"
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-4g-32k-sync.json"
reviseConfig $FILE T $((1<<32)) storage \"/tmp/yotta-disk-storage.test\" writesync true
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-16g-32k.json"
reviseConfig $FILE T $((1<<34)) storage \"/tmp/yotta-disk-storage.test\"
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-16g-32k-sync.json"
reviseConfig $FILE T $((1<<34)) storage \"/tmp/yotta-disk-storage.test\" writesync true
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-16g-1m.json"
reviseConfig $FILE T $((1<<34)) N $((128)) storage \"/tmp/yotta-disk-storage.test\" D $((1<<20))
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress

FILE="config-16g-1m-sync.json"
reviseConfig $FILE T $((1<<34)) N $((128)) storage \"/tmp/yotta-disk-storage.test\" D $((1<<20)) writesync true
echo $PLAY_CMD --config $FILE --test stress
$PLAY_CMD --config $FILE --test stress --format
time $PLAY_CMD --config $FILE --test stress
