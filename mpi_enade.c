// Arquivo: analisador_enade_final.c
// Versão final com a lógica correta, usando o arquivo da Pergunta 18 para
// responder sobre o Ensino Médio Técnico.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// --- CONFIGURAÇÃO ---
const char* NOMES_ARQUIVOS[] = {
    "DADOS_2021/microdados2021_arq5.txt",   // Sexo (TP_SEXO)
    "DADOS_2021/microdados2021_arq21.txt",  // Ação Afirmativa (QE_I15)
    // ATUALIZAÇÃO: Usando o arquivo da pergunta 18 para o tipo de Ensino Médio
    "DADOS_2021/microdados2021_arq24.txt",  // Modalidade Ensino Médio (QE_I18)
    "DADOS_2021/microdados2021_arq25.txt",  // Incentivo (QE_I19)
    "DADOS_2021/microdados2021_arq27.txt",  // Família com Superior (QE_I21)
    "DADOS_2021/microdados2021_arq28.txt",  // Livros (QE_I22)
    "DADOS_2021/microdados2021_arq29.txt"   // Horas de Estudo (QE_I23)
};
const int NUM_ARQUIVOS = 7;
const char* CODIGO_CURSO_ADS = "\"63045\"";

#define MAX_LINE_LEN 256
#define NUM_RESULTS 15 

/* MAPEAMENTO DOS CONTADORES DE RESULTADOS (ÍNDICES DO ARRAY):
 * 0: [Q1] Total de alunos em Ação Afirmativa
 * 1: [Q2] Total de MULHERES em Ação Afirmativa
 * 2: [Q3] Total de Mulheres em A.A. que fizeram Ensino Técnico (QE_I18, Resposta 'B')
 * 3: [Q4] Total de Mulheres em A.A. com incentivo dos PAIS (QE_I19, Resposta 'B')
 * 4: [Q5] Total de alunos em A.A. com família com curso superior (QE_I21, Resposta 'A')
 * ... (resto do mapeamento para livros e horas continua o mesmo)
 */

void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    
    while (linha != NULL) {
        char* campos[10];
        int i = 0;
        
        char* campo_saveptr;
        char* token = strtok_r(linha, ";", &campo_saveptr);
        while (token != NULL && i < 10) {
            campos[i++] = token;
            token = strtok_r(NULL, ";", &campo_saveptr);
        }

        if (i >= 9) {
            const char* sexo = campos[2];
            const char* acao_afirmativa = campos[3];
            const char* modalidade_ens_medio = campos[4]; // Vem da Pergunta 18 agora
            const char* incentivo = campos[5];
            const char* familia_superior = campos[6];
            const char* livros = campos[7];
            const char* horas_estudo = campos[8];

            int ehAcaoAfirmativa = (strcmp(acao_afirmativa, "\"A\"") != 0);
            int ehFeminino = (strcmp(sexo, "\"F\"") == 0);

            // Bloco 1: Perguntas com filtro de Ação Afirmativa
            if (ehAcaoAfirmativa) {
                resultados_locais[0]++; // Pergunta 1
                if (strcmp(familia_superior, "\"A\"") == 0) resultados_locais[4]++; // Pergunta 5
                
                if (ehFeminino) {
                    resultados_locais[1]++; // Pergunta 2
                    
                    // ATUALIZAÇÃO: Lógica da pergunta 3 agora funciona com os dados corretos
                    if (strcmp(modalidade_ens_medio, "\"B\"") == 0) resultados_locais[2]++;
                    
                    if (strcmp(incentivo, "\"B\"") == 0) resultados_locais[3]++;
                }
            }
            // Bloco 2: Pergunta sobre Livros (INDEPENDENTE)
            if (strcmp(livros, "\"A\"") == 0) resultados_locais[5]++;
            else if (strcmp(livros, "\"B\"") == 0) resultados_locais[6]++;
            else if (strcmp(livros, "\"C\"") == 0) resultados_locais[7]++;
            else if (strcmp(livros, "\"D\"") == 0) resultados_locais[8]++;
            else if (strcmp(livros, "\"E\"") == 0) resultados_locais[9]++;
            // Bloco 3: Pergunta sobre Horas de Estudo (INDEPENDENTE)
            if (strcmp(horas_estudo, "\"A\"") == 0) resultados_locais[10]++;
            else if (strcmp(horas_estudo, "\"B\"") == 0) resultados_locais[11]++;
            else if (strcmp(horas_estudo, "\"C\"") == 0) resultados_locais[12]++;
            else if (strcmp(horas_estudo, "\"D\"") == 0) resultados_locais[13]++;
            else if (strcmp(horas_estudo, "\"E\"") == 0) resultados_locais[14]++;
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

        size_t buffer_size = 1024 * 1024;
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

            if (strstr(linhas[0], CODIGO_CURSO_ADS) != NULL) {
                char linha_combinada[MAX_LINE_LEN * 2];
                char* ano = strtok(linhas[0], ";");
                char* curso = strtok(NULL, ";");
                sprintf(linha_combinada, "%s;%s", ano, curso);
                char* respostas[NUM_ARQUIVOS];
                strtok(linhas[0], ";"); strtok(NULL, ";"); respostas[0] = strtok(NULL, "\n\r");
                for(int i = 1; i < NUM_ARQUIVOS; i++) {
                     strtok(linhas[i], ";"); strtok(NULL, ";"); respostas[i] = strtok(NULL, "\n\r");
                }
                for(int i=0; i<NUM_ARQUIVOS; i++) {
                    strcat(linha_combinada, ";");
                    if (respostas[i]) strcat(linha_combinada, respostas[i]);
                }
                strcat(linha_combinada, "\n");
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
        printf("1. Total de alunos que ingressaram por Ações Afirmativas: %ld\n", totalAfirmativa);
        printf("2. Porcentagem de mulheres dentre os que ingressaram por Ações Afirmativas: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        // ATUALIZAÇÃO: Impressão da Pergunta 3 agora mostra o resultado calculado
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
