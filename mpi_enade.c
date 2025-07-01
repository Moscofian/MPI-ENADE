#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LEN 2048
#define ADS_GROUP_CODE 72

// Estrutura atualizada para incluir contadores de respostas nulas/inválidas.
typedef struct {
    long long total_students;
    long long female_students;
    long long tech_hs_students;
    long long affirmative_action_total;
    // Contadores de Incentivo
    long long incentive_parents, incentive_family, incentive_teachers, incentive_friends,
              incentive_religious, incentive_others, incentive_none, incentive_null;
    // Contadores de Família Graduada
    long long family_graduated_yes, family_graduated_no, family_graduated_null;
    // Contadores de Livros
    long long books_none, books_1_2, books_3_5, books_6_8, books_more_8, books_null;
    // Contadores de Horas de Estudo
    long long study_hours_none, study_hours_1_3, study_hours_4_7, study_hours_8_12,
              study_hours_more_12, study_hours_null;
} Results;

// Função para verificar se um curso já existe na lista para evitar duplicatas.
int is_course_in_list(int course_code, const int* list, int size) {
    for (int i = 0; i < size; i++) {
        if (list[i] == course_code) return 1;
    }
    return 0;
}

// Função para verificar se um código de curso pertence à lista final de cursos de ADS
int is_ads_course(int course_code, const int* ads_courses, int num_ads_courses) {
    for (int i = 0; i < num_ads_courses; i++) {
        if (ads_courses[i] == course_code) return 1;
    }
    return 0;
}

// Processa um único arquivo de dados e atualiza os contadores locais
void process_data_file(const char* filename, int rank, int world_size, const int* ads_courses, int num_ads_courses, Results* local_results) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        if (rank == 0) fprintf(stderr, "Aviso: Não foi possível abrir %s.\n", filename);
        return;
    }

    char line[MAX_LINE_LEN];
    long long line_number = 0;
    fgets(line, sizeof(line), fp); // Pula cabeçalho

    while (fgets(line, sizeof(line), fp)) {
        if (line_number % world_size == rank) {
            int year = 0, course_code = 0;
            char answer_str[2] = {0};

            // Este sscanf está CORRETO para os arquivos de resposta (ex: 2021;12345;"F")
            sscanf(line, "%d;%d;\"%1s\"", &year, &course_code, answer_str);
            
            if (is_ads_course(course_code, ads_courses, num_ads_courses)) {
                if (strcmp(filename, "DADOS/microdados2021_arq5.txt") == 0) {
                    local_results->total_students++;
                    if (answer_str[0] == 'F') local_results->female_students++;
                } else if (strcmp(filename, "DADOS/microdados2021_arq24.txt") == 0) {
                    if (answer_str[0] == 'B') local_results->tech_hs_students++;
                } else if (strcmp(filename, "DADOS/microdados2021_arq21.txt") == 0) {
                    if (answer_str[0] != 'A' && answer_str[0] != ' ' && answer_str[0] != '.') local_results->affirmative_action_total++;
                } else if (strcmp(filename, "DADOS/microdados2021_arq25.txt") == 0) {
                    switch (answer_str[0]) {
                        case 'A': local_results->incentive_none++; break; case 'B': local_results->incentive_parents++; break;
                        case 'C': local_results->incentive_family++; break; case 'D': local_results->incentive_teachers++; break;
                        case 'E': local_results->incentive_religious++; break; case 'F': local_results->incentive_friends++; break;
                        case 'G': local_results->incentive_others++; break; default: local_results->incentive_null++; break;
                    }
                } else if (strcmp(filename, "DADOS/microdados2021_arq27.txt") == 0) {
                    switch (answer_str[0]) {
                        case 'A': local_results->family_graduated_yes++; break; case 'B': local_results->family_graduated_no++; break;
                        default: local_results->family_graduated_null++; break;
                    }
                } else if (strcmp(filename, "DADOS/microdados2021_arq28.txt") == 0) {
                    switch (answer_str[0]) {
                        case 'A': local_results->books_none++; break; case 'B': local_results->books_1_2++; break;
                        case 'C': local_results->books_3_5++; break; case 'D': local_results->books_6_8++; break;
                        case 'E': local_results->books_more_8++; break; default: local_results->books_null++; break;
                    }
                } else if (strcmp(filename, "DADOS/microdados2021_arq29.txt") == 0) {
                    switch (answer_str[0]) {
                        case 'A': local_results->study_hours_none++; break; case 'B': local_results->study_hours_1_3++; break;
                        case 'C': local_results->study_hours_4_7++; break; case 'D': local_results->study_hours_8_12++; break;
                        case 'E': local_results->study_hours_more_12++; break; default: local_results->study_hours_null++; break;
                    }
                }
            }
        }
        line_number++;
    }
    fclose(fp);
}

// Função de impressão dos resultados (sem alterações)
void print_final_results(int num_ads_courses, const Results* final_results) {
    printf("\n===================================================\n");
    printf("   Análise dos Microdados do ENADE para ADS\n");
    printf("===================================================\n\n");
    
    printf("DETECÇÃO INICIAL:\n");
    printf("- Cursos de ADS (CO_GRUPO %d) capturados: %d\n", ADS_GROUP_CODE, num_ads_courses);
    printf("- Total de alunos de ADS detectados (baseado no arq5): %lld\n\n", final_results->total_students);
    printf("---------------------------------------------------\n\n");

    if (final_results->total_students == 0) {
        printf("Nenhum estudante do curso de ADS foi encontrado nos dados para análise.\n");
        return;
    }
    long long total_base_count = final_results->total_students;

    printf("1. Qual é a porcentagem de estudante do sexo Feminino?\n");
    double female_percentage = (total_base_count > 0) ? (double)final_results->female_students * 100.0 / total_base_count : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", final_results->female_students, female_percentage);

    printf("2. Qual é a porcentagem de estudantes que cursaram o ensino técnico no ensino médio?\n");
    double tech_hs_percentage = (total_base_count > 0) ? (double)final_results->tech_hs_students * 100.0 / total_base_count : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", final_results->tech_hs_students, tech_hs_percentage);

    printf("3. Qual é o percentual de alunos provenientes de ações afirmativas?\n");
    double affirmative_action_percentage = (total_base_count > 0) ? (double)final_results->affirmative_action_total * 100.0 / total_base_count : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", final_results->affirmative_action_total, affirmative_action_percentage);
    
    printf("4. Dos estudantes, quem deu incentivo para este estudante cursar o ADS?\n");
    long long total_incentive_valid = final_results->incentive_parents + final_results->incentive_family + final_results->incentive_teachers + final_results->incentive_friends + final_results->incentive_religious + final_results->incentive_others + final_results->incentive_none;
    if (total_incentive_valid > 0) {
        printf("   - Pais:                 %lld (%.2f%%)\n", final_results->incentive_parents, (double)final_results->incentive_parents * 100.0 / total_incentive_valid);
        printf("   - Outros familiares:    %lld (%.2f%%)\n", final_results->incentive_family, (double)final_results->incentive_family * 100.0 / total_incentive_valid);
        printf("   - Professores:          %lld (%.2f%%)\n", final_results->incentive_teachers, (double)final_results->incentive_teachers * 100.0 / total_incentive_valid);
        printf("   - Colegas/Amigos:       %lld (%.2f%%)\n", final_results->incentive_friends, (double)final_results->incentive_friends * 100.0 / total_incentive_valid);
        printf("   - Líder religioso:      %lld (%.2f%%)\n", final_results->incentive_religious, (double)final_results->incentive_religious * 100.0 / total_incentive_valid);
        printf("   - Outras pessoas:       %lld (%.2f%%)\n", final_results->incentive_others, (double)final_results->incentive_others * 100.0 / total_incentive_valid);
        printf("   - Ninguém:              %lld (%.2f%%)\n", final_results->incentive_none, (double)final_results->incentive_none * 100.0 / total_incentive_valid);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", final_results->incentive_null);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }

    printf("5. Quantos estudantes apresentaram familiares com o curso superior concluído?\n");
    long long total_family_valid = final_results->family_graduated_yes + final_results->family_graduated_no;
     if (total_family_valid > 0) {
        printf("   - Sim: %lld (%.2f%%)\n", final_results->family_graduated_yes, (double)final_results->family_graduated_yes * 100.0 / total_family_valid);
        printf("   - Não: %lld (%.2f%%)\n", final_results->family_graduated_no, (double)final_results->family_graduated_no * 100.0 / total_family_valid);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", final_results->family_graduated_null);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }

    printf("6. Quantos livros os alunos leram no ano do ENADE?\n");
    long long total_books_valid = final_results->books_none + final_results->books_1_2 + final_results->books_3_5 + final_results->books_6_8 + final_results->books_more_8;
    if (total_books_valid > 0) {
       printf("   - Nenhum:        %lld (%.2f%%)\n", final_results->books_none, (double)final_results->books_none * 100.0 / total_books_valid);
       printf("   - 1 ou 2:        %lld (%.2f%%)\n", final_results->books_1_2, (double)final_results->books_1_2 * 100.0 / total_books_valid);
       printf("   - 3 a 5:         %lld (%.2f%%)\n", final_results->books_3_5, (double)final_results->books_3_5 * 100.0 / total_books_valid);
       printf("   - 6 a 8:         %lld (%.2f%%)\n", final_results->books_6_8, (double)final_results->books_6_8 * 100.0 / total_books_valid);
       printf("   - Mais de 8:     %lld (%.2f%%)\n", final_results->books_more_8, (double)final_results->books_more_8 * 100.0 / total_books_valid);
       printf("   - Respostas Nulas/Inválidas: %lld\n\n", final_results->books_null);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }
    
    printf("7. Quantas horas na semana os estudantes se dedicaram aos estudos?\n");
    long long total_study_valid = final_results->study_hours_none + final_results->study_hours_1_3 + final_results->study_hours_4_7 + final_results->study_hours_8_12 + final_results->study_hours_more_12;
    if (total_study_valid > 0) {
        printf("   - Nenhuma (só aulas): %lld (%.2f%%)\n", final_results->study_hours_none, (double)final_results->study_hours_none * 100.0 / total_study_valid);
        printf("   - 1 a 3 horas:        %lld (%.2f%%)\n", final_results->study_hours_1_3, (double)final_results->study_hours_1_3 * 100.0 / total_study_valid);
        printf("   - 4 a 7 horas:        %lld (%.2f%%)\n", final_results->study_hours_4_7, (double)final_results->study_hours_4_7 * 100.0 / total_study_valid);
        printf("   - 8 a 12 horas:       %lld (%.2f%%)\n", final_results->study_hours_8_12, (double)final_results->study_hours_8_12 * 100.0 / total_study_valid);
        printf("   - Mais de 12 horas:   %lld (%.2f%%)\n", final_results->study_hours_more_12, (double)final_results->study_hours_more_12 * 100.0 / total_study_valid);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", final_results->study_hours_null);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }
}


int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    double start_time;

    if (rank == 0) {
        printf("Análise iniciada com %d processos.\n", world_size);
        start_time = MPI_Wtime();
    }
    
    int num_ads_courses = 0;
    int* ads_courses = NULL; 

    if (rank == 0) {
        int capacity = 0; 
        const char* arq1_path = "DADOS/microdados2021_arq1.txt";
        printf("Processo 0: Lendo a lista de cursos de ADS de '%s'...\n", arq1_path);
        
        FILE* fp = fopen(arq1_path, "r");
        if (!fp) {
            fprintf(stderr, "Erro fatal: não foi possível abrir '%s'.\n", arq1_path);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char line[MAX_LINE_LEN];
        fgets(line, sizeof(line), fp);

        while (fgets(line, sizeof(line), fp)) {
            int course_code = 0, group_code = 0;
            
            // ** CORREÇÃO CRÍTICA APLICADA **
            // Este formato lê os números puros do arquivo arq1.txt, sem esperar aspas.
            // O formato "%*[^;];" pula uma coluna inteira de qualquer tipo.
            int items_read = sscanf(line, "%*[^;];%d;%*[^;];%*[^;];%*[^;];%d", &course_code, &group_code);
            
            if (items_read >= 2 && group_code == ADS_GROUP_CODE) {
                // Adiciona o curso somente se ele for ÚNICO.
                if (!is_course_in_list(course_code, ads_courses, num_ads_courses)) {
                    if (num_ads_courses == capacity) {
                        int new_capacity = (capacity == 0) ? 100 : capacity * 2;
                        int* temp_ptr = realloc(ads_courses, new_capacity * sizeof(int));
                        if (!temp_ptr) {
                            fprintf(stderr, "Erro fatal: falha ao alocar memória.\n");
                            free(ads_courses);
                            MPI_Abort(MPI_COMM_WORLD, 1);
                        }
                        ads_courses = temp_ptr;
                        capacity = new_capacity;
                    }
                    ads_courses[num_ads_courses++] = course_code;
                }
            }
        }
        fclose(fp);
        printf("Processo 0: Encontrou %d cursos únicos. Distribuindo para os outros processos...\n", num_ads_courses);
    }

    MPI_Bcast(&num_ads_courses, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        ads_courses = (int*)malloc(num_ads_courses * sizeof(int));
        if (ads_courses == NULL && num_ads_courses > 0) {
             fprintf(stderr, "Processo %d: falha ao alocar memória.\n", rank);
             MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    MPI_Bcast(ads_courses, num_ads_courses, MPI_INT, 0, MPI_COMM_WORLD);
    
    MPI_Barrier(MPI_COMM_WORLD); 
    if (rank == 0) printf("\nIniciando a análise paralela dos arquivos de dados...\n\n");

    Results local_results = {0};
    const char* data_files[] = {
        "DADOS/microdados2021_arq5.txt", "DADOS/microdados2021_arq21.txt", "DADOS/microdados2021_arq24.txt",
        "DADOS/microdados2021_arq25.txt", "DADOS/microdados2021_arq27.txt", "DADOS/microdados2021_arq28.txt",
        "DADOS/microdados2021_arq29.txt"
    };
    int num_data_files = sizeof(data_files) / sizeof(data_files[0]);

    for (int i = 0; i < num_data_files; ++i) {
        if (rank == 0) printf("Analisando: %s...\n", data_files[i]);
        process_data_file(data_files[i], rank, world_size, ads_courses, num_ads_courses, &local_results);
        MPI_Barrier(MPI_COMM_WORLD); 
    }

    if (rank == 0) printf("\nAnálise paralela concluída. Agregando resultados...\n");

    Results final_results = {0};
    int num_struct_elements = sizeof(Results) / sizeof(long long);
    MPI_Reduce(&local_results, &final_results, num_struct_elements, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        double end_time = MPI_Wtime();
        print_final_results(num_ads_courses, &final_results);
        printf("---------------------------------------------------\n");
        printf("Análise concluída em %.4f segundos.\n", end_time - start_time);
    }
    
    free(ads_courses);
    MPI_Finalize();
    return 0;
}