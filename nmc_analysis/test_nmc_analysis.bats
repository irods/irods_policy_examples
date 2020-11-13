############################################
# NMC Analysis Policy test file
#
# $ git clone https://github.com/bats-core/bats-core
# $ bash bats-core/bin/bats test_nmc_analysis.bats
############################################

SOURCE_RESOURCE=demoResc
TARGET_RESOURCE=nmc-analysis
A=nmc
V=analysis
U=""
DELAY_SERVER_SLEEP=3

############

SINGLE_SLEEP=$(DELAY_SERVER_SLEEP+2))
DOUBLE_SLEEP=$((SINGLE_SLEEP*2))
COLLECTION=thecoll
FILENAME=thefile
SUBCOLLECTION=subcoll

setup () {
    echo "data" > ${FILENAME}
}

teardown () {
    rm ${FILENAME}
}

@test "tag a collection" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "tag a data object" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    # cleanup
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    [ $status -eq 0 ]
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "untag a collection" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ ! "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ ! "${lines[1]}" =~ " & ${FILENAME}" ]]
    # cleanup
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "untag a data object" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ ! "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ ! "${lines[1]}" =~ " & ${FILENAME}" ]]
    # cleanup
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "overwrite a tagged data object" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    echo "moardata" > ${FILENAME}
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    # cleanup
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    [ $status -eq 0 ]
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "overwrite a data object under a tagged collection" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    echo "moardata" > ${FILENAME}
    run iput -f ${FILENAME} ${COLLECTION}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    [ $status -eq 0 ]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "trim a tagged data object - DISALLOWED" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run itrim -n0 -N1 ${FILENAME}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    [ $status -eq 0 ]
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "trim a data object under a tagged collection - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run itrim -n0 -N1 ${COLLECTION}/${FILENAME}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "remove a tagged data object - DISALLOWED" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -f ${FILENAME}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    [ $status -eq 0 ]
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "remove a tagged collection - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -rf ${COLLECTION}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "remove a data object under a tagged collection - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -f ${COLLECTION}/${FILENAME}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "remove a collection under a tagged collection - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}/${SUBCOLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}/${SUBCOLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${SUBCOLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -rf ${COLLECTION}/${SUBCOLLECTION}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "remove a collection containing a tagged data object - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -d ${COLLECTION}/${FILENAME} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -rf ${COLLECTION}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -d ${COLLECTION}/${FILENAME} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "remove a collection containing a tagged collection - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}/${SUBCOLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}/${SUBCOLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION}/${SUBCOLLECTION} ${A} ${V} ${U}
    sleep ${DOUBLE_SLEEP}
    run ils -l ${COLLECTION}/${SUBCOLLECTION}/${FILENAME}
    [ $status -eq 0 ]
    [[ "${lines[0]}" =~ " 0 ${SOURCE_RESOURCE}" ]]
    [[ "${lines[0]}" =~ " & ${FILENAME}" ]]
    [[ "${lines[1]}" =~ " 1 ${TARGET_RESOURCE}" ]]
    [[ "${lines[1]}" =~ " & ${FILENAME}" ]]
    run irm -rf ${COLLECTION}
    [ $status -eq 3 ]
    [[ "${lines[0]}" =~ "-41000 SYS_DELETE_DISALLOWED" ]]
    # cleanup
    run imeta rm -C ${COLLECTION}/${SUBCOLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}

@test "untag an enqueued data object - DISALLOWED" {
    # main
    run iput -f ${FILENAME}
    [ $status -eq 0 ]
    run imeta add -d ${FILENAME} ${A} ${V} ${U}
    sleep ${SINGLE_SLEEP}
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    imeta ls -d ${FILENAME}
    [ $status -eq 4 ]
    [[ "${lines[0]}" =~ "SYS_DELETE_DISALLOWED" ]]
    # cleanup
    sleep ${DOUBLE_SLEEP}
    run imeta rm -d ${FILENAME} ${A} ${V} ${U}
    [ $status -eq 0 ]
    run irm -f ${FILENAME}
    [ $status -eq 0 ]
}

@test "untag a collection with an enqueued descendent data object - DISALLOWED" {
    # main
    run imkdir -p ${COLLECTION}
    [ $status -eq 0 ]
    run iput -f ${FILENAME} ${COLLECTION}
    [ $status -eq 0 ]
    run imeta add -C ${COLLECTION} ${A} ${V} ${U}
    sleep ${SINGLE_SLEEP}
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    [ $status -eq 4 ]
    [[ "${lines[0]}" =~ "SYS_DELETE_DISALLOWED" ]]
    # cleanup
    sleep ${DOUBLE_SLEEP}
    run imeta rm -C ${COLLECTION} ${A} ${V} ${U}
    run irm -rf ${COLLECTION}
    [ $status -eq 0 ]
}
