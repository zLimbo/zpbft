#!/bin/bash

client="219.228.148.154"
servers=(
    "219.228.148.45"
    "219.228.148.80"
    "219.228.148.89"
    "219.228.148.129"
    "219.228.148.178"
    "219.228.148.181"
    "219.228.148.222"
    "219.228.148.231"
)

src='.'
dst="/home/z/zpbft"

function deployClient() {
    printf "\n[deployClient]\n"

    printf "deploy client in %-16s ..." ${client}
    start=$(date +%s)

    if ! ssh z@${client} test -e ${dst}/config; then
        sshpass -p z ssh z@${client} mkdir -p ${dst}/config
    fi

    # sshpass -p z scp -r certs z@${client}:~/zpbft/certs
    sshpass -p z scp ${src}/bin/zpbft z@${client}:${dst}/zpbft
    sshpass -p z scp -r ${src}/config/config.json z@${client}:${dst}/config/config.json
    echo ${client} >config/local_ip.txt
    sshpass -p z scp -r ${src}/config/local_ip.txt z@${client}:${dst}/config/local_ip.txt

    end=$(date +%s)
    take=$((end - start))
    printf "\rdeploy client in %-16s ok, take %ds\n" ${client} ${take}
}

function deployServer() {
    printf "\n[deployServer]\n"

    for srv in ${servers[@]}; do
        printf "deploy server in %-16s ..." ${srv}
        start=$(date +%s)

        if ! ssh z@${srv} test -e ${dst}/config; then
            echo "mkdir ${dst}"
            sshpass -p z ssh z@${srv} mkdir -p ${dst}/config
        fi

        # sshpass -p z scp -r z@${srv}:${dst}/certs z@${srv}:${dst}/certs
        sshpass -p z scp ${src}/bin/zpbft z@${srv}:${dst}/zpbft
        sshpass -p z scp -r ${src}/config/config.json z@${srv}:${dst}/config/config.json
        echo ${srv} >config/local_ip.txt
        sshpass -p z scp -r ${src}/config/local_ip.txt z@${srv}:${dst}/config/local_ip.txt

        end=$(date +%s)
        take=$((end - start))
        printf "\rdeploy server in %-16s ok, take %ds\n" ${srv} ${take}
    done
}

if (($# == 0)); then
    echo
    echo echo "please input 'c', 's' or 'a' !"
    exit
fi

printf "\n[compile]\n"
printf "compile zpbft ..."
start=$(date +%s)

./build.sh

end=$(date +%s)
take=$((end - start))
printf "\rcompile zpbft ok, take %ds\n" ${take}

if [ $1 == "a" ]; then
    deployClient
    deployServer
elif [ $1 == "c" ]; then
    deployClient
elif [ $1 == "s" ]; then
    deployServer
else
    echo "please input 'c', 's' or 'a' !"
fi
