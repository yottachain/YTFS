# ! /bin/sh
 
# Get the fully qualified path to the script
case $0 in
    /*)
        SCRIPT="$0"
        ;;
    *)
        PWD=`pwd`
        SCRIPT="$PWD/$0"
        ;;
esac

# Resolve the true real path without any sym links.
CHANGED=true
while [ "X$CHANGED" != "X" ]
do
    # Change spaces to ":" so the tokens can be parsed.
    SAFESCRIPT=`echo $SCRIPT | sed -e 's; ;:;g'`
    # Get the real path to this script, resolving any symbolic links
    TOKENS=`echo $SAFESCRIPT | sed -e 's;/; ;g'`
    REALPATH=
    for C in $TOKENS; do
        # Change any ":" in the token back to a space.
        C=`echo $C | sed -e 's;:; ;g'`
        REALPATH="$REALPATH/$C"
        # If REALPATH is a sym link, resolve it.  Loop for nested links.
        while [ -h "$REALPATH" ] ; do
            LS="`ls -ld "$REALPATH"`"
            LINK="`expr "$LS" : '.*-> \(.*\)$'`"
            if expr "$LINK" : '/.*' > /dev/null; then
                # LINK is absolute.
                REALPATH="$LINK"
            else
                # LINK is relative.
                REALPATH="`dirname "$REALPATH"`""/$LINK"
            fi
        done
    done

    if [ "$REALPATH" = "$SCRIPT" ]
    then
        CHANGED=""
    else
        SCRIPT="$REALPATH"
    fi
done

# Get the location of the script.
REALDIR=`dirname "$REALPATH"`
# Normalize the path
REALDIR=`cd "${REALDIR}"; pwd`
cd $REALDIR
REALDIR="$REALDIR/cmdline"


echo "export SURFS_HOME=$REALDIR" >> /bin/surfs.ev
chmod 555 /bin/surfs.ev
cp ./cmdline/surmount.sh /bin/surmount
cp ./cmdline/surserver.sh /bin/surserver
cp ./cmdline/surfs.sh /bin/sfs
cp ./cmdline/surlan.sh /bin/surlan
chmod 555 /bin/surmount
chmod 555 /bin/surserver
chmod 555 /bin/sfs
chmod 555 /bin/surlan
./service/surlan.sh install

echo "Surfs client tools installed."

echo -n "Do you need to install the server?[N]:"
read line
if [ "$line" = "Y" -o "$line" = "y" ]
then 
    ./service/surserver.sh install
fi