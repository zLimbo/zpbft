#!/bin/bash

servers=(
    "10.11.6.117"
    "10.11.6.121"
    "10.11.7.71"
    "10.11.7.72"
    "10.11.7.73"
    "10.11.7.74"
    "10.11.7.75"
    "10.11.1.196"
)

src='.'
dst="~/zpbft"
user="tongxing"
passwd="tongxing"

function deployServer() {
    printf "\n[deployServer]\n"

    for srv in ${servers[@]}; do
        printf "deploy server in %-16s ..." ${srv}
        start=$(date +%s)

        # if ! sshpass -p ${passwd} ssh ${user}@${srv} test -e ${dst}/config; then
        #     echo "mkdir ${dst}"
        #     sshpass -p ${passwd} ssh ${user}@${srv} mkdir -p ${dst}/config
        # fi

        # sshpass -p ${passwd} scp -r certs ${user}@${srv}:~/zpbft/certs
        sshpass -p ${passwd} scp bin/zpbft ${user}@${srv}:${dst}/zpbft
        sshpass -p ${passwd} scp -r config/config.json ${user}@${srv}:${dst}/config/config.json
        echo ${srv} >config/local_ip.txt
        sshpass -p ${passwd} scp -r config/local_ip.txt ${user}@${srv}:${dst}/config/local_ip.txt

        end=$(date +%s)
        take=$((end - start))
        printf "\rdeploy server in %-16s ok, take %ds\n" ${srv} ${take}
    done
}

# if (($# == 0)); then
#     echo
#     echo echo "please input 'c', 's' or 'a' !"
#     exit
# fi

# printf "\n[compile]\n"
# printf "compile zpbft ..."
# start=$(date +%s)

# ./build.sh

# end=$(date +%s)
# take=$((end - start))
# printf "\rcompile zpbft ok, take %ds\n" ${take}

# 执行函数
deployServer
