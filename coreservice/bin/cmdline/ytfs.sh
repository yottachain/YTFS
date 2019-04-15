# ! /bin/sh
 # ---------------------surfs-----------------------
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
    elif [[ $var == 'wrapper.java.additional.1' ]]
    then
        java_opts="$java_opts ${val:0:${#val}-1}"
    elif [[ $var == 'wrapper.java.additional.2' ]]
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
    fi 
done < ../surfs.conf
 
mainclass="com.surfs.commons.client.SurfsUtils"

cmd="$java_cmd $java_opts -classpath $classpath $mainclass"
echo "cmd: $cmd"
$cmd