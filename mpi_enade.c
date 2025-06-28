// Arquivo: analisador_enade_logica_final.c
// Versão final com a lógica de análise ajustada para corresponder
// exatamente às perguntas e respostas da prova.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>

// --- CONFIGURAÇÃO ---
const char* NOMES_ARQUIVOS_PERGUNTAS[] = {
    "microdados2021_arq21.txt",  // QE_I15 (Ação Afirmativa)
    "microdados2021_arq5.txt",   // TP_SEXO (Sexo)
    "microdados2021_arq24.txt",  // QE_I18 (Modalidade Ensino Médio)
    "microdados2021_arq25.txt",  // QE_I19 (Incentivo)
    "microdados2021_arq27.txt",  // QE_I21 (Escolaridade Familiar)
    "microdados2021_arq28.txt",  // QE_I22 (Livros lidos)
    "microdados2021_arq29.txt"   // QE_I23 (Horas de estudo)
};
const char* ARQUIVO_MAPA_CURSOS = "microdados2021_arq1.txt";
const int NUM_ARQUIVOS_PERGUNTAS = 7;
const long CODIGO_GRUPO_ADS_NUM = 72; // TADS

#define MAX_LINE_LEN 256
#define NUM_RESULTS 20 // Ajustado para o número total de contadores

// Função para limpar aspas e espaços no início e no fim
void trim_quotes_and_spaces(char *str) {
    if (str == NULL || *str == '\0') return;
    char *start = str;
    while (isspace((unsigned char)*start) || *start == '"') start++;
    char *end = str + strlen(str) - 1;
    while (end > start && (isspace((unsigned char)*end) || *end == '"')) end--;
    *(end + 1) = '\0';
    if (start != str) memmove(str, start, strlen(start) + 1);
}

// Função para verificar se um curso está na lista de ADS
int is_ads_course(const char* co_curso, char** ads_course_list, int count) {
    for (int i = 0; i < count; i++) { if (strcmp(co_curso, ads_course_list[i]) == 0) return 1; }
    return 0;
}

// CORREÇÃO: Função de análise com a lógica exata das perguntas
void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    while (linha != NULL) {
        char* campos[10]; int i = 0;
        char* campo_saveptr;
        char* linha_copy = strdup(linha);
        
        char* token = strtok_r(linha_copy, ";", &campo_saveptr);
        while (token != NULL && i < 10) { campos[i++] = token; token = strtok_r(NULL, ";", &campo_saveptr); }
        
        if (i >= 9) {
            for(int k=2; k < i; k++) trim_quotes_and_spaces(campos[k]);

            const char* acao_afirmativa_raw = campos[2]; // QE_I15
            const char* sexo = campos[3];                // TP_SEXO
            const char* modalidade_em = campos[4];       // QE_I18
            const char* incentivo = campos[5];           // QE_I19
            const char* escolaridade_fam = campos[6];    // QE_I21
            const char* livros = campos[7];              // QE_I22
            const char* horas_estudo = campos[8];        // QE_I23

            // QE_I15: 'A' é "Não". B,C,D,E,F são "Sim".
            int ehAcaoAfirmativa = (strcmp(acao_afirmativa_raw, "A") != 0 && strcmp(acao_afirmativa_raw, ".") != 0 && strcmp(acao_afirmativa_raw, " ") != 0);
            int ehFeminino = (strcmp(sexo, "F") == 0);

            // Contabiliza livros e horas de estudo para todos os alunos de TADS
            if (strcmp(livros, "A") == 0) resultados_locais[10]++; else if (strcmp(livros, "B") == 0) resultados_locais[11]++; else if (strcmp(livros, "C") == 0) resultados_locais[12]++; else if (strcmp(livros, "D") == 0) resultados_locais[13]++; else if (strcmp(livros, "E") == 0) resultados_locais[14]++;
            if (strcmp(horas_estudo, "A") == 0) resultados_locais[15]++; else if (strcmp(horas_estudo, "B") == 0) resultados_locais[16]++; else if (strcmp(horas_estudo, "C") == 0) resultados_locais[17]++; else if (strcmp(horas_estudo, "D") == 0) resultados_locais[18]++; else if (strcmp(horas_estudo, "E") == 0) resultados_locais[19]++;
            
            if (ehAcaoAfirmativa) {
                // Pergunta 1: Total de alunos por A.A.
                resultados_locais[0]++;

                // QE_I21: 'A' é "Sim". 'B' é "Não".
                if (strcmp(escolaridade_fam, "A") == 0) {
                    // Pergunta 5: Familiares com curso superior (dentre os de A.A.)
                    resultados_locais[9]++;
                }

                if (ehFeminino) {
                    // Pergunta 2: Total de mulheres em A.A.
                    resultados_locais[1]++;

                    // QE_I18: 'B' é "Profissionalizante Técnico".
                    if (strcmp(modalidade_em, "B") == 0) {
                        // Pergunta 3: Mulheres de A.A. que fizeram técnico.
                        resultados_locais[2]++;
                    }

                    // QE_I19: Incentivo.
                    if (strcmp(incentivo, "B") == 0) resultados_locais[3]++;      // Pais
                    else if (strcmp(incentivo, "C") == 0) resultados_locais[4]++; // Outros familiares
                    else if (strcmp(incentivo, "D") == 0) resultados_locais[5]++; // Professores
                    else if (strcmp(incentivo, "F") == 0) resultados_locais[6]++; // Colegas/Amigos
                    else if (strcmp(incentivo, "E") == 0) resultados_locais[7]++; // Líder religioso
                    else if (strcmp(incentivo, "G") == 0) resultados_locais[8]++; // Outras pessoas
                }
            }
        }
        free(linha_copy);
        linha = strtok_r(NULL, "\n", &linha_saveptr);
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, n_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
    long resultados_locais[NUM_RESULTS] = {0};

    if (rank == 0) {
        printf("Mestre (Rank 0): Mapeando cursos de ADS (Grupo %ld).\n", CODIGO_GRUPO_ADS_NUM);
        FILE* fp_mapa = fopen(ARQUIVO_MAPA_CURSOS, "r");
        if (!fp_mapa) { fprintf(stderr, "Erro fatal: Falha ao abrir mapa de cursos '%s'.\n", ARQUIVO_MAPA_CURSOS); MPI_Abort(MPI_COMM_WORLD, 1); }
        char** ads_courses_list = NULL; int ads_courses_count = 0; char line[MAX_LINE_LEN];
        fgets(line, sizeof(line), fp_mapa);
        while (fgets(line, sizeof(line), fp_mapa)) {
            char* line_copy = strdup(line); char* saveptr1;
            strtok_r(line_copy, ";", &saveptr1); char* co_curso_str = strtok_r(NULL, ";", &saveptr1);
            strtok_r(NULL, ";", &saveptr1); strtok_r(NULL, ";", &saveptr1); strtok_r(NULL, ";", &saveptr1);
            char* co_grupo_str = strtok_r(NULL, ";", &saveptr1);
            if (co_grupo_str && co_curso_str && atol(co_grupo_str) == CODIGO_GRUPO_ADS_NUM) {
                trim_quotes_and_spaces(co_curso_str);
                int found = 0;
                for(int i = 0; i < ads_courses_count; i++) if(strcmp(ads_courses_list[i], co_curso_str) == 0) {found = 1; break;}
                if(!found) {
                    ads_courses_count++;
                    ads_courses_list = realloc(ads_courses_list, ads_courses_count * sizeof(char*));
                    ads_courses_list[ads_courses_count - 1] = strdup(co_curso_str);
                }
            }
            free(line_copy);
        }
        fclose(fp_mapa);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de ADS encontrados. Unificando dados...\n", ads_courses_count);
        FILE* readers[NUM_ARQUIVOS_PERGUNTAS];
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
            readers[i] = fopen(NOMES_ARQUIVOS_PERGUNTAS[i], "r");
            if (!readers[i]) { fprintf(stderr, "Erro fatal: Falha ao abrir o arquivo %s\n", NOMES_ARQUIVOS_PERGUNTAS[i]); MPI_Abort(MPI_COMM_WORLD, 1); }
            fgets(line, sizeof(line), readers[i]);
        }
        size_t buffer_size = 1024 * 1024 * 20; char* dados_unificados = malloc(buffer_size); size_t current_pos = 0; dados_unificados[0] = '\0';
        char linhas[NUM_ARQUIVOS_PERGUNTAS][MAX_LINE_LEN];
        while(fgets(linhas[0], sizeof(linhas[0]), readers[0]) != NULL) {
            int linha_valida = 1;
            for(int i = 1; i < NUM_ARQUIVOS_PERGUNTAS; i++) { if (fgets(linhas[i], sizeof(linhas[i]), readers[i]) == NULL) { linha_valida = 0; break; } }
            if (!linha_valida) break;
            char* curso_copy = strdup(linhas[0]); char* saveptr2;
            char* ano = strtok_r(curso_copy, ";", &saveptr2); char* curso = strtok_r(NULL, ";", &saveptr2);
            trim_quotes_and_spaces(curso);
            if (curso && is_ads_course(curso, ads_courses_list, ads_courses_count)) {
                char linha_combinada[MAX_LINE_LEN * 2] = {0};
                trim_quotes_and_spaces(ano); sprintf(linha_combinada, "%s;%s", ano, curso);
                for(int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
                    char* resp_copy = strdup(linhas[i]); char* saveptr3;
                    strtok_r(resp_copy, ";", &saveptr3); strtok_r(NULL, ";", &saveptr3); char* resposta = strtok_r(NULL, "\n\r", &saveptr3);
                    strcat(linha_combinada, ";"); if (resposta) strcat(linha_combinada, resposta);
                    free(resp_copy);
                }
                strcat(linha_combinada, "\n");
                if (current_pos + strlen(linha_combinada) + 1 > buffer_size) { buffer_size *= 2; dados_unificados = realloc(dados_unificados, buffer_size); }
                strcpy(dados_unificados + current_pos, linha_combinada);
                current_pos += strlen(linha_combinada);
            }
            free(curso_copy);
        }
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) fclose(readers[i]);
        for (int i = 0; i < ads_courses_count; i++) free(ads_courses_list[i]); free(ads_courses_list);
        printf("Mestre (Rank 0): Leitura concluída. Total de %zu bytes para TADS. Distribuindo trabalho...\n", current_pos);
        long base_chunk_size = current_pos / n_procs; long start_offset = 0;
        for (int i = 1; i < n_procs; i++) {
            long end_offset = start_offset + base_chunk_size;
            if (end_offset >= current_pos) end_offset = current_pos;
            else {
                while (end_offset < current_pos && dados_unificados[end_offset] != '\n') end_offset++;
                if(end_offset < current_pos) end_offset++;
            }
            long size_to_send = end_offset - start_offset;
            MPI_Send(&size_to_send, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);
            if (size_to_send > 0) MPI_Send(dados_unificados + start_offset, size_to_send, MPI_CHAR, i, 0, MPI_COMM_WORLD);
            start_offset = end_offset;
        }
        long my_chunk_size = start_offset >= current_pos ? 0 : current_pos - start_offset;
        if(my_chunk_size > 0) {
            char* my_bloco = malloc(my_chunk_size + 1);
            memcpy(my_bloco, dados_unificados + start_offset, my_chunk_size);
            my_bloco[my_chunk_size] = '\0';
            analisar_bloco(my_bloco, resultados_locais);
            free(my_bloco);
        }
        free(dados_unificados);
    } else {
        long size_to_recv = 0;
        MPI_Recv(&size_to_recv, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (size_to_recv > 0) {
            char* bloco_recebido = malloc(size_to_recv + 1);
            MPI_Recv(bloco_recebido, size_to_recv, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            bloco_recebido[size_to_recv] = '\0';
            analisar_bloco(bloco_recebido, resultados_locais);
            free(bloco_recebido);
        }
    }
    long resultados_globais[NUM_RESULTS] = {0};
    MPI_Reduce(resultados_locais, resultados_globais, NUM_RESULTS, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        long totalAfirmativa = resultados_globais[0];
        long mulheresAfirmativa = resultados_globais[1];
        long mulheresTecnicoAA = resultados_globais[2];
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (CURSO: TADS) ---\n\n");
        printf("1. Quantos alunos se matricularam no curso por meio de ações afirmativas?\n   - Total: %ld\n", totalAfirmativa);
        printf("\n2. Qual é a porcentagem de estudantes do sexo Feminino que se matricularam por ações afirmativas?\n   - Porcentagem: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        printf("\n3. Qual é a porcentagem de estudantes (sexo Feminino e de A.A.) que cursaram o ensino médio técnico?\n   - Porcentagem: %.2f%%\n", mulheresAfirmativa > 0 ? (double)mulheresTecnicoAA / mulheresAfirmativa * 100.0 : 0);
        printf("\n4. Dos estudantes do sexo Feminino e das ações afirmativas, quem deu maior incentivo para cursar a graduação?\n");
        printf("   - Pais:........................ %ld\n", resultados_globais[3]);
        printf("   - Outros familiares:........... %ld\n", resultados_globais[4]);
        printf("   - Professores:................. %ld\n", resultados_globais[5]);
        printf("   - Colegas/Amigos:.............. %ld\n", resultados_globais[6]);
        printf("   - Líder ou repr. religioso:.... %ld\n", resultados_globais[7]);
        printf("   - Outras pessoas:.............. %ld\n", resultados_globais[8]);
        printf("\n5. Dos estudantes de ações afirmativas, quantos apresentaram familiares com o curso superior concluído?\n   - Total: %ld\n", resultados_globais[9]);
        printf("\n6. Quantos livros os alunos de TADS leram neste ano?\n");
        printf("   - Nenhum:...................... %ld\n", resultados_globais[10]);
        printf("   - Um ou dois:.................. %ld\n", resultados_globais[11]);
        printf("   - Três a cinco:................ %ld\n", resultados_globais[12]);
        printf("   - Seis a oito:................. %ld\n", resultados_globais[13]);
        printf("   - Mais de oito:................ %ld\n", resultados_globais[14]);
        printf("\n7. Quantas horas semanais os estudantes de TADS se dedicaram aos estudos?\n");
        printf("   - Nenhuma:..................... %ld\n", resultados_globais[15]);
        printf("   - De uma a três:............... %ld\n", resultados_globais[16]);
        printf("   - De quatro a sete:............ %ld\n", resultados_globais[17]);
        printf("   - De oito a doze:.............. %ld\n", resultados_globais[18]);
        printf("   - Mais de doze:................ %ld\n", resultados_globais[19]);
    }
    MPI_Finalize();
    return 0;
}
