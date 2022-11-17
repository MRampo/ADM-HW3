words=(Italy Spain France England United\ States)
for n in "${words[@]}";
do :
    
    count=$(cut -f 3,4,8 all_tsv.tsv | grep "$n" | wc -l)
    
    sum=$(cut -f 3,4,8 all_tsv.tsv | grep "$n" | awk '{sum+=$1 } END{print (sum)}')
    mean=$(($sum/$count))


    total_wish=$(cut -f 3,4,8 all_tsv.tsv | grep "$n" | awk '{sum+=$2 } END{print sum}')

    echo $n 
    echo total number of places in $n : $count
    echo Average visitors of $n : $mean
    echo Who wants to visit $n in total: $total_wish
done