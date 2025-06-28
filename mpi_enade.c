// Arquivo: analisador_enade_final_corrigido.c
// Versão com a correção crítica na ordem das colunas do arquivo de mapa de cursos.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// --- CONFIGURAÇÃO ---
const char* ARQUIVO_MAPA_CURSOS = "DADOS_2021/microdados2021_arq1.txt";
const char* NOMES_ARQUIVOS_PERGUNTAS[] = {
    "DADOS_2021/microdados2021_arq5.txt",
    "DADOS_2021/microdados2021_arq21.txt",
    "DADOS_2021/microdados2021_arq24.txt",
    "DADOS_2021/microdados2021_arq25.txt",
    "DADOS_2021/microdados2021_arq27.txt",
    "DADOS_2021/microdados2021_arq28.txt",
    "DADOS_2021/microdados2021_arq29.txt"
};
const int NUM_ARQUIVOS_PERGUNTAS = 7;
const char* CODIGO_GRUPO_ADS = "72";

#define MAX_LINE_LEN 256
#define NUM_RESULTS 15

// Função auxiliar para verificar se um co_curso pertence à lista de cursos de ADS
int is_ads_course(const char* co_curso, char** ads_course_list, int count) {
    for (int i = 0; i < count; i++) {
        if (strcmp(co_curso, ads_course_list[i]) == 0) return 1;
    }
    return 0;
}

// A função de análise do bloco não precisa de mudanças.
void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    while (linha != NULL) {
        char* campos[10]; int i = 0;
        char* campo_saveptr;
        char* token = strtok_r(linha, ";", &campo_saveptr);
        while (token != NULL && i < 10) { campos[i++] = token; token = strtok_r(NULL, ";", &campo_saveptr); }
        if (i >= 9) {
            const char* sexo = campos[2], *acao_afirmativa = campos[3], *modalidade_ens_medio = campos[4], *incentivo = campos[5], *familia_superior = campos[6], *livros = campos[7], *horas_estudo = campos[8];
            int ehAcaoAfirmativa = (strcmp(acao_afirmativa, "\"A\"") != 0), ehFeminino = (strcmp(sexo, "\"F\"") == 0);
            if (ehAcaoAfirmativa) {
                resultados_locais[0]++; if (strcmp(familia_superior, "\"A\"") == 0) resultados_locais[4]++;
                if (ehFeminino) { resultados_locais[1]++; if (strcmp(modalidade_ens_medio, "\"B\"") == 0) resultados_locais[2]++; if (strcmp(incentivo, "\"B\"") == 0) resultados_locais[3]++; }
            }
            if (strcmp(livros, "\"A\"") == 0) resultados_locais[5]++; else if (strcmp(livros, "\"B\"") == 0) resultados_locais[6]++; else if (strcmp(livros, "\"C\"") == 0) resultados_locais[7]++; else if (strcmp(livros, "\"D\"") == 0) resultados_locais[8]++; else if (strcmp(livros, "\"E\"") == 0) resultados_locais[9]++;
            if (strcmp(horas_estudo, "\"A\"") == 0) resultados_locais[10]++; else if (strcmp(horas_estudo, "\"B\"") == 0) resultados_locais[11]++; else if (strcmp(horas_estudo, "\"C\"") == 0) resultados_locais[12]++; else if (strcmp(horas_estudo, "\"D\"") == 0) resultados_locais[13]++; else if (strcmp(horas_estudo, "\"E\"") == 0) resultados_locais[14]++;
        }
        linha = strtok_r(NULL, "\n", &linha_saveptr);
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, n_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs);
    long resultados_locais[NUM_RESULTS] = {0};

    // ETAPA 1: MESTRE (RANK 0) MAPEIA OS CURSOS DE ADS
    if (rank == 0) {
        printf("Mestre (Rank 0): Lendo arquivo de mapa de cursos (%s)...\n", ARQUIVO_MAPA_CURSOS);
        FILE* fp_mapa = fopen(ARQUIVO_MAPA_CURSOS, "r");
        if (!fp_mapa) { fprintf(stderr, "Erro: Falha ao abrir mapa de cursos.\n"); MPI_Abort(MPI_COMM_WORLD, 1); }
        
        char** ads_courses_list = NULL;
        int ads_courses_count = 0;
        char line[MAX_LINE_LEN];
        fgets(line, sizeof(line), fp_mapa);

        while (fgets(line, sizeof(line), fp_mapa)) {
            char* co_curso_str;
            char* co_grupo_str;
            char* line_copy = strdup(line); // Usar uma cópia para strtok não destruir a original

            // ATUALIZAÇÃO CRÍTICA: Corrigindo a ordem das colunas com base na imagem.
            strtok(line_copy, ";");                  // 1. NU_ANO
            co_curso_str = strtok(NULL, ";");      // 2. CO_CURSO
            strtok(NULL, ";");                     // 3. CO_IES
            strtok(NULL, ";");                     // 4. CO_CATEGAD
            strtok(NULL, ";");                     // 5. CO_ORGACAD
            co_grupo_str = strtok(NULL, ";");      // 6. CO_GRUPO (agora está correto!)

            if (co_grupo_str && co_curso_str && strcmp(co_grupo_str, CODIGO_GRUPO_ADS) == 0) {
                ads_courses_count++;
                ads_courses_list = realloc(ads_courses_list, ads_courses_count * sizeof(char*));
                ads_courses_list[ads_courses_count - 1] = strdup(co_curso_str);
            }
            free(line_copy); // Liberar a memória da cópia da linha
        }
        fclose(fp_mapa);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de ADS encontrados.\n", ads_courses_count);
        printf("Mestre (Rank 0): Lendo e unificando dados das perguntas...\n");

        // O restante do código (unificação, distribuição, análise e impressão) não precisa de alterações
        // pois ele já depende da 'ads_courses_list' que agora está correta.
        
        // ETAPA 2: MESTRE UNIFICA OS DADOS DAS PERGUNTAS USANDO O MAPA
        FILE* readers[NUM_ARQUIVOS_PERGUNTAS];
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
            readers[i] = fopen(NOMES_ARQUIVOS_PERGUNTAS[i], "r");
            if (!readers[i]) { fprintf(stderr, "Erro: Falha ao abrir o arquivo %s\n", NOMES_ARQUIVOS_PERGUNTAS[i]); MPI_Abort(MPI_COMM_WORLD, 1); }
            fgets(line, sizeof(line), readers[i]);
        }
        size_t buffer_size = 1024 * 1024;
        char* dados_unificados = malloc(buffer_size);
        size_t current_pos = 0;
        dados_unificados[0] = '\0';
        char linhas[NUM_ARQUIVOS_PERGUNTAS][MAX_LINE_LEN];
        while(fgets(linhas[0], sizeof(linhas[0]), readers[0]) != NULL) {
            int linha_valida = 1;
            for(int i = 1; i < NUM_ARQUIVOS_PERGUNTAS; i++) { if (fgets(linhas[i], sizeof(linhas[i]), readers[i]) == NULL) { linha_valida = 0; break; } }
            if (!linha_valida) break;
            char temp_line[MAX_LINE_LEN];
            strcpy(temp_line, linhas[0]);
            char* ano = strtok(temp_line, ";");
            char* curso = strtok(NULL, ";");
            if (curso && is_ads_course(curso, ads_courses_list, ads_courses_count)) {
                char linha_combinada[MAX_LINE_LEN * 2];
                sprintf(linha_combinada, "%s;%s", ano, curso);
                char* respostas[NUM_ARQUIVOS_PERGUNTAS];
                strtok(linhas[0], ";"); strtok(NULL, ";"); respostas[0] = strtok(NULL, "\n\r");
                for(int i = 1; i < NUM_ARQUIVOS_PERGUNTAS; i++) { strtok(linhas[i], ";"); strtok(NULL, ";"); respostas[i] = strtok(NULL, "\n\r"); }
                for(int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) { strcat(linha_combinada, ";"); if (respostas[i]) strcat(linha_combinada, respostas[i]); }
                strcat(linha_combinada, "\n");
                if (current_pos + strlen(linha_combinada) + 1 > buffer_size) {
                    buffer_size *= 2;
                    dados_unificados = realloc(dados_unificados, buffer_size);
                }
                strcpy(dados_unificados + current_pos, linha_combinada);
                current_pos += strlen(linha_combinada);
            }
        }
        for (int i=0; i<NUM_ARQUIVOS_PERGUNTAS; i++) fclose(readers[i]);
        for (int i=0; i<ads_courses_count; i++) free(ads_courses_list[i]);
        free(ads_courses_list);
        printf("Mestre (Rank 0): Leitura concluída. Total de %zu bytes para TADS. Distribuindo trabalho...\n", current_pos);
        long chunk_size = current_pos / n_procs;
        for (int i = 1; i < n_procs; i++) {
            long start = i * chunk_size;
            long size_to_send = (i == n_procs - 1) ? (current_pos - start) : chunk_size;
            MPI_Send(&size_to_send, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);
            MPI_Send(dados_unificados + start, size_to_send, MPI_CHAR, i, 0, MPI_COMM_WORLD);
        }
        long my_chunk_size = chunk_size;
        char* my_bloco = malloc(my_chunk_size + 1);
        strncpy(my_bloco, dados_unificados, my_chunk_size);
        my_bloco[my_chunk_size] = '\0';
        analisar_bloco(my_bloco, resultados_locais);
        free(my_bloco);
        free(dados_unificados);
    } 
    else {
        long size_to_recv;
        MPI_Recv(&size_to_recv, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* bloco_recebido = malloc(size_to_recv + 1);
        MPI_Recv(bloco_recebido, size_to_recv, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        bloco_recebido[size_to_recv] = '\0';
        analisar_bloco(bloco_recebido, resultados_locais);
        free(bloco_recebido);
    }
    long resultados_globais[NUM_RESULTS] = {0};
    MPI_Reduce(resultados_locais, resultados_globais, NUM_RESULTS, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) {
        long totalAfirmativa = resultados_globais[0], mulheresAfirmativa = resultados_globais[1], mulheresTecnico = resultados_globais[2];
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (TADS) ---\n\n");
        printf("1. Total de alunos que ingressaram por Ações Afirmativas: %ld\n", totalAfirmativa);
        printf("2. Porcentagem de mulheres dentre os que ingressaram por Ações Afirmativas: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        printf("3. Dentre as mulheres de A.A., porcentagem que concluiu Ensino Técnico: %.2f%%\n", mulheresAfirmativa > 0 ? (double)mulheresTecnico / mulheresAfirmativa * 100.0 : 0);
        printf("4. Dentre as mulheres de A.A., total que recebeu maior incentivo dos Pais: %ld\n", resultados_globais[3]);
        printf("5. Dentre os alunos de A.A., total com familiares com curso superior: %ld\n", resultados_globais[4]);
        printf("\n6. Quantos livros os alunos de TADS leram no ano?\n");
        printf("   - Nenhum (A):.................. %ld\n", resultados_globais[5]);
        printf("   - Um ou dois (B):.............. %ld\n", resultados_globais[6]);
        printf("   - Três a cinco (C):............ %ld\n", resultados_globais[7]);
        printf("   - Seis a oito (D):............. %ld\n", resultados_globais[8]);
        printf("   - Mais de oito (E):............ %ld\n", resultados_globais[9]);
        printf("\n7. Quantas horas semanais os alunos de TADS se dedicaram aos estudos?\n");
        printf("   - Nenhuma (A):................. %ld\n", resultados_globais[10]);
        printf("   - De uma a três (B):........... %ld\n", resultados_globais[11]);
        printf("   - De quatro a sete (C):........ %ld\n", resultados_globais[12]);
        printf("   - De oito a doze (D):.......... %ld\n", resultados_globais[13]);
        printf("   - Mais de doze (E):............ %ld\n", resultados_globais[14]);
    }
    MPI_Finalize();
    return 0;
}
