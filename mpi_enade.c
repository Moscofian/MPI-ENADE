// Arquivo: analisador_enade_final_com_percentual.c
// Descrição: Versão final do analisador de dados do ENADE 2021.
//            Processa cada arquivo de forma independente e apresenta os resultados
//            com totais por pergunta e porcentagens de resposta.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>

// =================================================================================
// --- SEÇÃO DE CONFIGURAÇÃO ---
// =================================================================================

const char* QUESTION_FILES[] = {
    "microdados2021_arq21.txt", // 0: QE_I15 (Ação Afirmativa)
    "microdados2021_arq5.txt",  // 1: TP_SEXO (Impossível de usar para cruzar dados)
    "microdados2021_arq24.txt", // 2: QE_I18 (Impossível de usar)
    "microdados2021_arq25.txt", // 3: QE_I19 (Impossível de usar)
    "microdados2021_arq27.txt", // 4: QE_I21 (Impossível de usar)
    "microdados2021_arq28.txt", // 5: QE_I22 (Livros lidos)
    "microdados2021_arq29.txt"  // 6: QE_I23 (Horas de estudo)
};
enum QuestionIndex { AA = 0, SEX = 1, HS_TYPE = 2, INCENTIVE = 3, FAMILY_EDU = 4, BOOKS = 5, STUDY_HOURS = 6 };

const int NUM_QUESTION_FILES = 7;
const char* COURSE_MAP_FILE = "microdados2021_arq1.txt";
const long TADS_GROUP_CODE = 72;

#define MAX_LINE_LEN 512
#define COURSE_CODE_LEN 10

// =================================================================================
// --- ESTRUTURAS DE DADOS E FUNÇÕES AUXILIARES ---
// =================================================================================

// Estrutura para os resultados, agora com contador para 'Não' em A.A.
typedef struct {
    long aa_students_yes;
    long aa_students_no; // Adicionado para calcular o total de respostas
    // Livros
    long books_none; long books_1_2; long books_3_5; long books_6_8; long books_over_8;
    // Horas de Estudo
    long study_none; long study_1_3; long study_4_7; long study_8_12; long study_over_12;
} AnalysisResults;

void trim_quotes_and_spaces(char *str) {
    if (str == NULL || *str == '\0') return;
    char *start = str;
    while (isspace((unsigned char)*start) || *start == '"') start++;
    char *end = str + strlen(str) - 1;
    while (end > start && (isspace((unsigned char)*end) || *end == '"')) end--;
    *(end + 1) = '\0';
    if (start != str) memmove(str, start, strlen(start) + 1);
}

int is_tads_course(const char* course_code, char (*tads_list)[COURSE_CODE_LEN], int count) {
    for (int i = 0; i < count; i++) {
        if (strcmp(course_code, tads_list[i]) == 0) return 1;
    }
    return 0;
}

void process_single_file(const char* filename, int file_index, char (*tads_codes)[COURSE_CODE_LEN], int tads_count, AnalysisResults* results) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "Aviso: Não foi possível abrir o arquivo %s. Ignorando.\n", filename);
        return;
    }
    char line[MAX_LINE_LEN];
    fgets(line, sizeof(line), fp);
    while (fgets(line, sizeof(line), fp)) {
        char* line_copy = strdup(line);
        char* saveptr; strtok_r(line_copy, ";", &saveptr);
        char* co_curso_str = strtok_r(NULL, ";", &saveptr);
        char* answer = strtok_r(NULL, "\r\n", &saveptr);
        if (!co_curso_str || !answer) { free(line_copy); continue; }
        trim_quotes_and_spaces(co_curso_str);
        trim_quotes_and_spaces(answer);
        if (is_tads_course(co_curso_str, tads_codes, tads_count)) {
            switch (file_index) {
                case AA: // Ação Afirmativa
                    if (strcmp(answer, "A") == 0) {
                        results->aa_students_no++;
                    } else if (strlen(answer) > 0 && strcmp(answer, ".") != 0) {
                        results->aa_students_yes++;
                    }
                    break;
                case BOOKS:
                    if (strcmp(answer, "A") == 0) results->books_none++;
                    else if (strcmp(answer, "B") == 0) results->books_1_2++;
                    else if (strcmp(answer, "C") == 0) results->books_3_5++;
                    else if (strcmp(answer, "D") == 0) results->books_6_8++;
                    else if (strcmp(answer, "E") == 0) results->books_over_8++;
                    break;
                case STUDY_HOURS:
                    if (strcmp(answer, "A") == 0) results->study_none++;
                    else if (strcmp(answer, "B") == 0) results->study_1_3++;
                    else if (strcmp(answer, "C") == 0) results->study_4_7++;
                    else if (strcmp(answer, "D") == 0) results->study_8_12++;
                    else if (strcmp(answer, "E") == 0) results->study_over_12++;
                    break;
            }
        }
        free(line_copy);
    }
    fclose(fp);
}

// =================================================================================
// --- FUNÇÃO PRINCIPAL ---
// =================================================================================
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    // ETAPAS 1 e 2: Mapeamento e Broadcast dos cursos (sem alterações)
    char (*tads_course_codes)[COURSE_CODE_LEN] = NULL;
    int tads_course_count = 0;
    if (rank == 0) {
        printf("Mestre (Rank 0): Mapeando cursos de TADS (Grupo %ld)...\n", TADS_GROUP_CODE);
        FILE* fp = fopen(COURSE_MAP_FILE, "r"); if (!fp) { MPI_Abort(MPI_COMM_WORLD, 1); }
        char line[MAX_LINE_LEN]; fgets(line, sizeof(line), fp);
        while (fgets(line, sizeof(line), fp)) {
            char* line_copy = strdup(line); char* saveptr; strtok_r(line_copy, ";", &saveptr);
            char* co_curso_str = strtok_r(NULL, ";", &saveptr);
            strtok_r(NULL, ";", &saveptr); strtok_r(NULL, ";", &saveptr); strtok_r(NULL, ";", &saveptr);
            char* co_grupo_str = strtok_r(NULL, ";", &saveptr);
            if (co_grupo_str && co_curso_str && atol(co_grupo_str) == TADS_GROUP_CODE) {
                trim_quotes_and_spaces(co_curso_str); int found = 0;
                for (int i = 0; i < tads_course_count; i++) { if (strcmp(tads_course_codes[i], co_curso_str) == 0) { found = 1; break; } }
                if (!found) {
                    tads_course_count++; tads_course_codes = realloc(tads_course_codes, tads_course_count * sizeof(*tads_course_codes));
                    strncpy(tads_course_codes[tads_course_count - 1], co_curso_str, COURSE_CODE_LEN - 1);
                    tads_course_codes[tads_course_count-1][COURSE_CODE_LEN-1] = '\0';
                }
            } free(line_copy);
        } fclose(fp);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de TADS únicos encontrados.\n", tads_course_count);
    }
    MPI_Bcast(&tads_course_count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (rank != 0) { tads_course_codes = malloc(tads_course_count * sizeof(*tads_course_codes)); }
    MPI_Bcast(tads_course_codes, tads_course_count * COURSE_CODE_LEN, MPI_CHAR, 0, MPI_COMM_WORLD);

    // ETAPAS 3 e 4: Processamento e Agregação (sem alterações na lógica principal)
    AnalysisResults local_results = {0};
    for (int i = rank; i < NUM_QUESTION_FILES; i += size) {
        process_single_file(QUESTION_FILES[i], i, tads_course_codes, tads_course_count, &local_results);
    }
    free(tads_course_codes);
    AnalysisResults global_results = {0};
    MPI_Reduce(&local_results, &global_results, sizeof(AnalysisResults)/sizeof(long), MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // --- ETAPA 5: APRESENTAÇÃO DOS RESULTADOS (RANK 0) ---
    if (rank == 0) {
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (CURSO: TADS) ---\n\n");
        printf("AVISO IMPORTANTE: Devido à estrutura dos dados, apenas as perguntas\n");
        printf("que podem ser respondidas de forma independente são mostradas abaixo.\n\n");
        
        // --- Pergunta 1: Ações Afirmativas ---
        long total_aa_responses = global_results.aa_students_yes + global_results.aa_students_no;
        printf("1. Análise sobre ingresso por Ações Afirmativas (A.A.)\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_aa_responses);
        printf("   - Sim, ingressou por A.A.: ............ %ld (%.2f%%)\n", global_results.aa_students_yes, (total_aa_responses > 0) ? (double)global_results.aa_students_yes / total_aa_responses * 100.0 : 0.0);
        printf("   - Não, ingressou por ampla concorrência: %ld (%.2f%%)\n\n", global_results.aa_students_no, (total_aa_responses > 0) ? (double)global_results.aa_students_no / total_aa_responses * 100.0 : 0.0);
        
        // --- Pergunta 2: Livros Lidos ---
        long total_books_responses = global_results.books_none + global_results.books_1_2 + global_results.books_3_5 + global_results.books_6_8 + global_results.books_over_8;
        printf("2. Quantos livros os alunos de TADS leram neste ano?\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_books_responses);
        printf("   - Nenhum: ............................... %ld (%.2f%%)\n", global_results.books_none, (total_books_responses > 0) ? (double)global_results.books_none / total_books_responses * 100.0 : 0.0);
        printf("   - Um ou dois: ........................... %ld (%.2f%%)\n", global_results.books_1_2, (total_books_responses > 0) ? (double)global_results.books_1_2 / total_books_responses * 100.0 : 0.0);
        printf("   - Três a cinco: ......................... %ld (%.2f%%)\n", global_results.books_3_5, (total_books_responses > 0) ? (double)global_results.books_3_5 / total_books_responses * 100.0 : 0.0);
        printf("   - Seis a oito: .......................... %ld (%.2f%%)\n", global_results.books_6_8, (total_books_responses > 0) ? (double)global_results.books_6_8 / total_books_responses * 100.0 : 0.0);
        printf("   - Mais de oito: ......................... %ld (%.2f%%)\n\n", global_results.books_over_8, (total_books_responses > 0) ? (double)global_results.books_over_8 / total_books_responses * 100.0 : 0.0);
        
        // --- Pergunta 3: Horas de Estudo ---
        long total_study_responses = global_results.study_none + global_results.study_1_3 + global_results.study_4_7 + global_results.study_8_12 + global_results.study_over_12;
        printf("3. Quantas horas semanais os estudantes de TADS se dedicaram aos estudos?\n");
        printf("   Total de respostas válidas para esta pergunta: %ld\n", total_study_responses);
        printf("   - Nenhuma: .............................. %ld (%.2f%%)\n", global_results.study_none, (total_study_responses > 0) ? (double)global_results.study_none / total_study_responses * 100.0 : 0.0);
        printf("   - De uma a três: ........................ %ld (%.2f%%)\n", global_results.study_1_3, (total_study_responses > 0) ? (double)global_results.study_1_3 / total_study_responses * 100.0 : 0.0);
        printf("   - De quatro a sete: ..................... %ld (%.2f%%)\n", global_results.study_4_7, (total_study_responses > 0) ? (double)global_results.study_4_7 / total_study_responses * 100.0 : 0.0);
        printf("   - De oito a doze: ....................... %ld (%.2f%%)\n", global_results.study_8_12, (total_study_responses > 0) ? (double)global_results.study_8_12 / total_study_responses * 100.0 : 0.0);
        printf("   - Mais de doze: ......................... %ld (%.2f%%)\n\n", global_results.study_over_12, (total_study_responses > 0) ? (double)global_results.study_over_12 / total_study_responses * 100.0 : 0.0);
        
        printf("-----------------------------------------------------------\n");
        printf("As seguintes perguntas não puderam ser respondidas pois exigem o cruzamento\n");
        printf("de dados de um mesmo aluno entre múltiplos arquivos, o que não é possível.\n");
        printf("-----------------------------------------------------------\n");
    }

    MPI_Finalize();
    return 0;
}
