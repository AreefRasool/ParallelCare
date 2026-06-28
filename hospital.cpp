#include "hospital.h"

#define TOTAL_PATIENTS 50000
#define NUM_THREADS 4

typedef struct {
    int id;
    char name[50];
    int age;
    char disease[40];
    int priority;
    int token;
    int doctorId;
    char ward[30];
    long long processingScore;
} Patient;

typedef struct {
    Patient *patients;
    int start;
    int end;
} ThreadData;

Patient patients[TOTAL_PATIENTS];
Patient tempPatients[TOTAL_PATIENTS];
Patient sequentialData[TOTAL_PATIENTS];
Patient pthreadData[TOTAL_PATIENTS];
Patient openmpData[TOTAL_PATIENTS];
Patient mpiData[TOTAL_PATIENTS];

int generated = 0;

char diseases[6][40] = {
    "Heart Attack", "Accident", "Fever",
    "Fracture", "Chest Pain", "Flu"
};

void assignDoctorAndWard(Patient *p) {
    if (p->priority == 1) {
        p->doctorId = 1 + rand() % 4;
        strcpy(p->ward, "ICU");
    }
    else if (p->priority == 2) {
        p->doctorId = 5 + rand() % 4;
        strcpy(p->ward, "Critical Care");
    }
    else {
        p->doctorId = 9 + rand() % 4;
        strcpy(p->ward, "General Ward");
    }
}

void generatePatients() {
    for (int i = 0; i < TOTAL_PATIENTS; i++) {
        patients[i].id = i + 1;
        sprintf(patients[i].name, "Patient_%d", i + 1);
        patients[i].age = 10 + rand() % 80;
        strcpy(patients[i].disease, diseases[rand() % 6]);
        patients[i].priority = 1 + rand() % 3;
        patients[i].token = i + 1;
        patients[i].processingScore = 0;

        assignDoctorAndWard(&patients[i]);
    }

    generated = 1;
    printf("\n%d patient records generated successfully.\n", TOTAL_PATIENTS);
}

void copyPatients(Patient dest[], Patient src[]) {
    for (int i = 0; i < TOTAL_PATIENTS; i++) {
        dest[i] = src[i];
    }
}

int comparePriority(const void *a, const void *b) {
    Patient *p1 = (Patient *)a;
    Patient *p2 = (Patient *)b;

    if (p1->priority != p2->priority)
        return p1->priority - p2->priority;

    return p1->id - p2->id;
}

void processSinglePatient(Patient *p) {
    long long score = 0;

    for (int i = 0; i < 3000; i++) {
        score += (p->age * p->priority + p->doctorId + i) % 100;
    }

    p->processingScore = score;
}

void sequentialProcessing(Patient data[]) {
    for (int i = 0; i < TOTAL_PATIENTS; i++) {
        processSinglePatient(&data[i]);
    }
}

void *threadProcessing(void *arg) {
    ThreadData *data = (ThreadData *)arg;

    for (int i = data->start; i < data->end; i++) {
        processSinglePatient(&data->patients[i]);
    }

    pthread_exit(NULL);
}

void posixThreadProcessing(Patient data[]) {
    pthread_t threads[NUM_THREADS];
    ThreadData threadData[NUM_THREADS];

    int chunkSize = TOTAL_PATIENTS / NUM_THREADS;

    for (int i = 0; i < NUM_THREADS; i++) {
        threadData[i].patients = data;
        threadData[i].start = i * chunkSize;
        threadData[i].end = (i == NUM_THREADS - 1)
                            ? TOTAL_PATIENTS
                            : (i + 1) * chunkSize;

        pthread_create(&threads[i], NULL, threadProcessing, &threadData[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
}

void openMPProcessing(Patient data[]) {
    #pragma omp parallel for
    for (int i = 0; i < TOTAL_PATIENTS; i++) {
        processSinglePatient(&data[i]);
    }
}

long long mpiProcessing(Patient data[]) {
    int rank, size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int chunkSize = TOTAL_PATIENTS / size;
    int start = rank * chunkSize;
    int end = (rank == size - 1) ? TOTAL_PATIENTS : start + chunkSize;

    long long localScore = 0;

    for (int i = start; i < end; i++) {
        processSinglePatient(&data[i]);
        localScore += data[i].processingScore;
    }

    long long globalScore = 0;

    MPI_Reduce(
        &localScore,
        &globalScore,
        1,
        MPI_LONG_LONG,
        MPI_SUM,
        0,
        MPI_COMM_WORLD
    );

    return globalScore;
}

void displaySamplePatients() {
    copyPatients(tempPatients, patients);
    qsort(tempPatients, TOTAL_PATIENTS, sizeof(Patient), comparePriority);

    printf("\nSample Patients After Priority Sorting:\n\n");

    printf("%-8s %-16s %-6s %-18s %-10s %-12s %-14s %-15s\n",
           "ID", "Name", "Age", "Disease", "Priority", "Token", "Doctor ID", "Ward");

    printf("------------------------------------------------------------------------------------------------------\n");

    for (int i = 0; i < 10; i++) {
        printf("%-8d %-16s %-6d %-18s %-10d %-12d %-14d %-15s\n",
               tempPatients[i].id,
               tempPatients[i].name,
               tempPatients[i].age,
               tempPatients[i].disease,
               tempPatients[i].priority,
               tempPatients[i].token,
               tempPatients[i].doctorId,
               tempPatients[i].ward);
    }

    printf("\nPriority Meaning:");
    printf("\n1 = Emergency");
    printf("\n2 = Serious");
    printf("\n3 = Normal\n");
}

void displayStatisticsDashboard() {
    int emergency = 0, serious = 0, normal = 0;
    int icu = 0, critical = 0, general = 0;

    for (int i = 0; i < TOTAL_PATIENTS; i++) {
        if (patients[i].priority == 1) {
            emergency++;
            icu++;
        }
        else if (patients[i].priority == 2) {
            serious++;
            critical++;
        }
        else {
            normal++;
            general++;
        }
    }

    printf("\n================ Hospital Statistics Dashboard ================\n");

    printf("\nTotal Patients       : %d", TOTAL_PATIENTS);

    printf("\n\nPriority Wise Patients:");
    printf("\nEmergency Patients   : %d", emergency);
    printf("\nSerious Patients     : %d", serious);
    printf("\nNormal Patients      : %d", normal);

    printf("\n\nWard Allocation:");
    printf("\nICU Patients         : %d", icu);
    printf("\nCritical Care        : %d", critical);
    printf("\nGeneral Ward         : %d", general);

    printf("\n\nDoctors Available:");
    printf("\nEmergency Doctors    : Doctor 1 to 4");
    printf("\nCritical Doctors     : Doctor 5 to 8");
    printf("\nGeneral Doctors      : Doctor 9 to 12");

    printf("\n\n===============================================================\n");
}

void saveResultsToCSV(double seqTime, double pthreadTime, double ompTime, double mpiTime) {
    FILE *file = fopen("performance_results.csv", "w");

    if (file == NULL) {
        printf("\nError creating CSV file.\n");
        return;
    }

    fprintf(file, "Method,Execution Time,Speedup\n");
    fprintf(file, "Sequential,%f,1.00\n", seqTime);
    fprintf(file, "POSIX Threads,%f,%f\n", pthreadTime, seqTime / pthreadTime);
    fprintf(file, "OpenMP,%f,%f\n", ompTime, seqTime / ompTime);
    fprintf(file, "MPI,%f,%f\n", mpiTime, seqTime / mpiTime);

    fclose(file);

    printf("\nResults saved in performance_results.csv\n");
}

void runPerformanceComparison() {
    int rank, size;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    double sequentialTime = 0.0;
    double pthreadTime = 0.0;
    double openmpTime = 0.0;
    double mpiTime = 0.0;
    long long mpiTotalScore = 0;

    if (rank == 0) {
        copyPatients(sequentialData, patients);
        copyPatients(pthreadData, patients);
        copyPatients(openmpData, patients);
        copyPatients(mpiData, patients);

        qsort(sequentialData, TOTAL_PATIENTS, sizeof(Patient), comparePriority);
        qsort(pthreadData, TOTAL_PATIENTS, sizeof(Patient), comparePriority);
        qsort(openmpData, TOTAL_PATIENTS, sizeof(Patient), comparePriority);
        qsort(mpiData, TOTAL_PATIENTS, sizeof(Patient), comparePriority);

        double startSeq = omp_get_wtime();
        sequentialProcessing(sequentialData);
        double endSeq = omp_get_wtime();
        sequentialTime = endSeq - startSeq;

        double startPthread = omp_get_wtime();
        posixThreadProcessing(pthreadData);
        double endPthread = omp_get_wtime();
        pthreadTime = endPthread - startPthread;

        double startOMP = omp_get_wtime();
        openMPProcessing(openmpData);
        double endOMP = omp_get_wtime();
        openmpTime = endOMP - startOMP;
    }

    MPI_Bcast(mpiData, TOTAL_PATIENTS * sizeof(Patient), MPI_BYTE, 0, MPI_COMM_WORLD);

    MPI_Barrier(MPI_COMM_WORLD);
    double startMPI = MPI_Wtime();

    mpiTotalScore = mpiProcessing(mpiData);

    MPI_Barrier(MPI_COMM_WORLD);
    double endMPI = MPI_Wtime();

    mpiTime = endMPI - startMPI;

    if (rank == 0) {
        printf("\n================ Performance Results ================\n");

        printf("\nTotal Patients Processed : %d", TOTAL_PATIENTS);
        printf("\nPOSIX Threads Used       : %d", NUM_THREADS);
        printf("\nOpenMP Max Threads       : %d", omp_get_max_threads());
        printf("\nMPI Processes Used       : %d", size);

        printf("\n\nSequential Processing Time : %.6f seconds", sequentialTime);
        printf("\nPOSIX Threads Time         : %.6f seconds", pthreadTime);
        printf("\nOpenMP Processing Time     : %.6f seconds", openmpTime);
        printf("\nMPI Processing Time        : %.6f seconds", mpiTime);

        printf("\n\n================ Speedup Analysis ================\n");

        printf("\nPOSIX Threads Speedup : %.2fx", sequentialTime / pthreadTime);
        printf("\nOpenMP Speedup        : %.2fx", sequentialTime / openmpTime);
        printf("\nMPI Speedup           : %.2fx", sequentialTime / mpiTime);

        printf("\n\nMPI Total Processing Score : %lld", mpiTotalScore);

        printf("\n\n====================================================\n");

        saveResultsToCSV(sequentialTime, pthreadTime, openmpTime, mpiTime);
    }
}

void displayProjectInfo() {
    printf("\n================ Project Information ================\n");

    printf("\nProject Title:");
    printf("\nParallel Smart Hospital Queue Management System");

    printf("\n\nMain Concept:");
    printf("\nSame hospital queue processing task is executed using");
    printf("\nSequential Processing, POSIX Threads, OpenMP and MPI.");

    printf("\n\nModules:");
    printf("\n1. Patient Record Generation");
    printf("\n2. Priority-Based Queue Sorting");
    printf("\n3. Doctor Assignment");
    printf("\n4. Ward Allocation");
    printf("\n5. Hospital Statistics Dashboard");
    printf("\n6. Sequential Queue Processing");
    printf("\n7. POSIX Thread-Based Processing");
    printf("\n8. OpenMP Parallel Processing");
    printf("\n9. MPI Distributed Processing");
    printf("\n10. Execution Time and Speedup Analysis");
    printf("\n11. CSV Export and Result Storage");

    printf("\n\n=====================================================\n");
}

int runHospitalSystem(int argc, char *argv[]) {
    int choice;
    int rank;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    srand(10);

    if (rank == 0) {
        printf("=====================================================\n");
        printf(" Parallel Smart Hospital Queue Management System\n");
        printf(" Using Sequential, POSIX Threads, OpenMP and MPI\n");
        printf("=====================================================\n");
    }

    do {
        if (rank == 0) {
            printf("\n\n================ Main Menu ================");
            printf("\n1. Generate Patient Records");
            printf("\n2. Display Sample Patients");
            printf("\n3. Display Statistics Dashboard");
            printf("\n4. Run Performance Comparison");
            printf("\n5. Display Project Information");
            printf("\n6. Exit");

            printf("\n\nEnter your choice: ");
            scanf("%d", &choice);
        }

        MPI_Bcast(&choice, 1, MPI_INT, 0, MPI_COMM_WORLD);

        if (choice == 1) {
            if (rank == 0) {
                generatePatients();
            }
        }
        else if (choice == 2) {
            if (rank == 0) {
                if (!generated)
                    printf("\nPlease generate patient records first.\n");
                else
                    displaySamplePatients();
            }
        }
        else if (choice == 3) {
            if (rank == 0) {
                if (!generated)
                    printf("\nPlease generate patient records first.\n");
                else
                    displayStatisticsDashboard();
            }
        }
        else if (choice == 4) {
            if (rank == 0 && !generated) {
                printf("\nPlease generate patient records first.\n");
            }

            MPI_Bcast(&generated, 1, MPI_INT, 0, MPI_COMM_WORLD);

            if (generated) {
                runPerformanceComparison();
            }
        }
        else if (choice == 5) {
            if (rank == 0) {
                displayProjectInfo();
            }
        }
        else if (choice == 6) {
            if (rank == 0) {
                printf("\nExiting program...\n");
            }
        }
        else {
            if (rank == 0) {
                printf("\nInvalid choice. Please try again.\n");
            }
        }

    } while (choice != 6);

    MPI_Finalize();

    return 0;
}
