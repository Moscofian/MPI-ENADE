#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// --- CONFIGURAÇÃO ---
// Lista de arquivos de texto que o programa irá ler
const char* NOMES_ARQUIVOS[] = {
    "DADOS_2021/microdados2021_arq5.txt",   // Resposta estará no campo 3
    "DADOS_2021/microdados2021_arq21.txt",  // Resposta estará no campo 4
    "DADOS_2021/microdados2021_arq23.txt",  // Resposta estará no campo 5
    "DADOS_2021/microdados2021_arq25.txt",  // Resposta estará no campo 6
    "DADOS_2021/microdados2021_arq27.txt",  // Resposta estará no campo 7
    "DADOS_2021/microdados2021_arq28.txt",  // Resposta estará no campo 8
    "DADOS_2021/microdados2021_arq29.txt"   // Resposta estará no campo 9
};
const int NUM_ARQUIVOS = 7;
const char* CODIGO_CURSO_ADS = "\"63045\""; // Código do curso, com aspas como no arquivo

#define MAX_LINE_LEN 256
#define NUM_RESULTS 7 // Total de perguntas a serem respondidas

// Função que analisa um bloco de texto com dados já unificados
void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    
    while (linha != NULL) {
        char* campos[10]; // Deve ser suficiente para 9 campos
        int i = 0;
        
        char* campo_saveptr;
        char* token = strtok_r(linha, ";", &campo_saveptr);
        while (token != NULL && i < 10) {
            campos[i++] = token;
            token = strtok_r(NULL, ";", &campo_saveptr);
        }

        if (i >= 9) { // Garante que a linha tem todos os campos esperados
            // Extrai os valores relevantes da linha UMA VEZ para clareza
            const char* sexo = campos[2];
            const char* acao_afirmativa = campos[3];
            const char* ensino_medio = campos[4];
            const char* incentivo = campos[5];
            const char* familia_superior = campos[6];
            const char* livros = campos[7];
            const char* horas_estudo = campos[8];

            int ehAcaoAfirmativa = (strcmp(acao_afirmativa, "\"A\"") == 0);
            int ehFeminino = (strcmp(sexo, "\"F\"") == 0);

            // Bloco 1: Perguntas com filtro de Ação Afirmativa
            if (ehAcaoAfirmativa) {
                resultados_locais[0]++; // Pergunta 1: Total alunos em A.A.
                if (strcmp(familia_superior, "\"B\"") == 0) resultados_locais[4]++; // Pergunta 5
                
                // Sub-bloco: Perguntas para Mulheres DENTRO de A.A.
                if (ehFeminino) {
                    resultados_locais[1]++; // Pergunta 2: Contagem de mulheres em A.A.
                    if (strcmp(ensino_medio, "\"B\"") == 0) resultados_locais[2]++; // Pergunta 3
                    if (strcmp(incentivo, "\"A\"") == 0) resultados_locais[3]++; // Pergunta 4
                }
            }
            // Bloco 2: Pergunta sobre Livros (INDEPENDENTE)
            if (strcmp(livros, "\"C\"") == 0) resultados_locais[5]++;
            
            // Bloco 3: Pergunta sobre Horas de Estudo (INDEPENDENTE)
            if (strcmp(horas_estudo, "\"C\"") == 0) resultados_locais[6]++;
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

    // ETAPA 1: MESTRE (RANK 0) FAZ TODA A LEITURA E UNIFICAÇÃO EM MEMÓRIA
    if (rank == 0) {
        printf("Mestre (Rank 0): Lendo e unificando dados...\n");
        FILE* readers[NUM_ARQUIVOS];
        for (int i = 0; i < NUM_ARQUIVOS; i++) {
            readers[i] = fopen(NOMES_ARQUIVOS[i], "r");
            if (!readers[i]) {
                fprintf(stderr, "Erro: Falha ao abrir o arquivo %s\n", NOMES_ARQUIVOS[i]);
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            char header_buffer[MAX_LINE_LEN];
            fgets(header_buffer, sizeof(header_buffer), readers[i]);
        }

        size_t buffer_size = 1024 * 1024; // Começa com 1MB
        char* dados_unificados = malloc(buffer_size);
        if (!dados_unificados) { MPI_Abort(MPI_COMM_WORLD, 1); }
        size_t current_pos = 0;
        dados_unificados[0] = '\0';
        
        char linhas[NUM_ARQUIVOS][MAX_LINE_LEN];
        while(fgets(linhas[0], sizeof(linhas[0]), readers[0]) != NULL) {
            int linha_valida = 1;
            for(int i = 1; i < NUM_ARQUIVOS; i++) {
                if (fgets(linhas[i], sizeof(linhas[i]), readers[i]) == NULL) {
                    linha_valida = 0;
                    break;
                }
            }
            if (!linha_valida) break;

            // Filtra pela linha do curso de TADS
            if (strstr(linhas[0], CODIGO_CURSO_ADS) != NULL) {
                char linha_combinada[MAX_LINE_LEN * 2];
                // Pega ano e curso da primeira linha
                char *ano = strtok(linhas[0], ";");
                char *curso = strtok(NULL, ";");
                sprintf(linha_combinada, "%s;%s", ano, curso);

                // Pega a resposta de cada uma das 7 linhas
                char* respostas[NUM_ARQUIVOS];
                strtok(linhas[0], ";"); strtok(NULL, ";"); respostas[0] = strtok(NULL, "\n\r");
                for(int i = 1; i < NUM_ARQUIVOS; i++) {
                     strtok(linhas[i], ";"); strtok(NULL, ";"); respostas[i] = strtok(NULL, "\n\r");
                }
                
                // Concatena as respostas na linha combinada
                for(int i=0; i<NUM_ARQUIVOS; i++) {
                    strcat(linha_combinada, ";");
                    if (respostas[i]) strcat(linha_combinada, respostas[i]);
                }
                strcat(linha_combinada, "\n");

                // Adiciona ao buffer gigante, realocando memória se necessário
                if (current_pos + strlen(linha_combinada) + 1 > buffer_size) {
                    buffer_size *= 2;
                    dados_unificados = realloc(dados_unificados, buffer_size);
                    if (!dados_unificados) { MPI_Abort(MPI_COMM_WORLD, 1); }
                }
                strcpy(dados_unificados + current_pos, linha_combinada);
                current_pos += strlen(linha_combinada);
            }
        }
        
        for (int i=0; i<NUM_ARQUIVOS; i++) fclose(readers[i]);
        printf("Mestre (Rank 0): Leitura concluída. Total de %zu bytes para TADS. Distribuindo trabalho...\n", current_pos);

        // ETAPA 2: MESTRE DISTRIBUI O TRABALHO
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
    // ETAPA 3: TRABALHADORES RECEBEM E PROCESSAM
    else {
        long size_to_recv;
        MPI_Recv(&size_to_recv, 1, MPI_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        char* bloco_recebido = malloc(size_to_recv + 1);
        MPI_Recv(bloco_recebido, size_to_recv, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        bloco_recebido[size_to_recv] = '\0';
        analisar_bloco(bloco_recebido, resultados_locais);
        free(bloco_recebido);
    }

    // ETAPA 4: AGREGAÇÃO DOS RESULTADOS E IMPRESSÃO
    long resultados_globais[NUM_RESULTS] = {0};
    MPI_Reduce(resultados_locais, resultados_globais, NUM_RESULTS, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        long totalAfirmativa = resultados_globais[0];
        long mulheresAfirmativa = resultados_globais[1];
        long mulheresTecnico = resultados_globais[2];
        
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (TADS) ---\n\n");
        printf("1. Total de alunos por ações afirmativas: %ld\n", totalAfirmativa);
        printf("2. Porcentagem de mulheres em ações afirmativas: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        printf("3. Dentre as mulheres de ações afirmativas, que cursaram ensino técnico: %.2f%%\n", mulheresAfirmativa > 0 ? (double)mulheresTecnico / mulheresAfirmativa * 100.0 : 0);
        printf("4. Incentivo (Pais) para mulheres de ações afirmativas: %ld\n", resultados_globais[3]);
        printf("5. Alunos de ações afirmativas com familiares com curso superior: %ld\n", resultados_globais[4]);
        printf("6. Alunos que leram de 3 a 5 livros (Cat. 'C'): %ld\n", resultados_globais[5]);
        printf("7. Alunos que estudaram de 8 a 12h/semana (Cat. 'C'): %ld\n", resultados_globais[6]);
    }
    
    MPI_Finalize();
    return 0;
}
