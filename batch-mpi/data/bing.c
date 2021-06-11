#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


int main(int argc, char** argv) {
    // Initialize the MPI environment
    MPI_Init(NULL, NULL);
    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    printf("rank = %d and size = %d\n", world_rank, world_size);
    printf("argc =%d from %d\n", argc, world_rank);
    sleep(1);
    for(int i=0;i < argc; i++){
        printf("argv[%d] = %s\n",i, argv[i]);
        sleep(1);
    }
    for(int i = 1; i < argc; i++){
        FILE *fp; // declaration of file pointer
        fp =fopen(argv[i], "r"); // opening of file
        if (!fp) // checking of error
                printf("no file pointer\n");
        else{
            char con[1000]; // variable to read the content
            while (fgets(con,1000, fp)!=NULL)// reading file content
            printf("%s",con);
            fclose(fp); // closing file
        }
    }
    int number;
    if (world_rank == 0) {
        number = 11;
        printf("process 0 sending\n");
        MPI_Send(&number, 1, MPI_INT, 1, 10, MPI_COMM_WORLD);
        sleep(5);

    } else if (world_rank < world_size) {
        printf("process %d recieving now\n", world_rank);
        MPI_Status stat;
        MPI_Recv(&number, 1, MPI_INT, world_rank-1, 10, MPI_COMM_WORLD, &stat);
        printf("Process %d received number %d from process %d\n",world_rank,
            number, world_rank-1);
        fflush(stdout);
        number++;
        if (world_rank + 1 < world_size){
            printf("process %d sending\n", world_rank);
            MPI_Send(&number, 1, MPI_INT, world_rank+1, 10, MPI_COMM_WORLD);
        sleep(5);
        }
    }
    int global_sum;
    global_sum = -1;
    MPI_Reduce(&number, &global_sum, 1, MPI_INT, MPI_SUM, 0,
            MPI_COMM_WORLD);

    // Print the result
    printf("Total sum = %d\n", global_sum);
    fflush(stdout);
    sleep(5);
    
}