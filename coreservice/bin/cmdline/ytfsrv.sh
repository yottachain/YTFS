# ! /bin/sh
# ---------------------surfs-server-----------------------
source surfs.ev

if [ -z $SURFS_HOME ]; then  
    echo "Environment variable 'SURFS_HOME' not found "
    exit 0;
fi 

echo "SURFS_HOME:$SURFS_HOME"
cd $SURFS_HOME

while  IFS='=' read var val
do
    if [[ $var == 'wrapper.java.command' ]]
    then
        java_cmd=${val:0:${#val}-1}
    elif [[ $var == 'wrapper.java.initmemory' ]]
    then
        java_opts=-Xms${val:0:${#val}-1}M
    elif [[ $var == 'wrapper.java.maxmemory' ]]
    then
        java_opts="$java_opts -Xmx${val:0:${#val}-1}M"
    elif [[ $var == 'wrapper.java.additional.1' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.2' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.3' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.4' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.5' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.6' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.classpath.1' ]]
    then
        classpath=${val:0:${#val}-1}
    elif [[ $var == 'wrapper.java.classpath.2' ]]
    then
        classpath="$classpath:${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.classpath.3' ]]
    then
        classpath="$classpath:${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.classpath.4' ]]
    then
        classpath="$classpath:${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.app.parameter.1' ]]
    then
        java_args=${val:0:${#val}-1}
    elif [[ $var == 'wrapper.app.parameter.2' ]]
    then
        java_args="$java_args ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.mainclass' ]]
    then
        mainclass=${val:0:${#val}-1}
    fi 
done < ../surfs.conf
 
cmd="$java_cmd $java_opts -classpath $classpath $mainclass $java_args"
echo "cmd: $cmd"
$cmd &