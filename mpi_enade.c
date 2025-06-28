// Arquivo: analisador_enade_final_comentado.c
// Este programa unifica e analisa os dados do ENADE 2021 para TADS em paralelo.
// O modelo usado é "Gerente/Trabalhador" (Master/Worker).
// O Processo 0 (Gerente) lê todos os arquivos e distribui o trabalho.
// Todos os processos (incluindo o Gerente) analisam uma parte dos dados.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// --- CONFIGURAÇÃO ---
// Explicação: Define os nomes dos arquivos que o programa precisa ler.
// A ordem aqui é importante para a lógica de extração das respostas.
const char* ARQUIVO_MAPA_CURSOS = "DADOS_2021/microdados2021_arq1.txt"; // Usado para mapear CO_GRUPO -> CO_CURSO
const char* NOMES_ARQUIVOS_PERGUNTAS[] = {
    "DADOS_2021/microdados2021_arq5.txt",   // Pergunta sobre Sexo
    "DADOS_2021/microdados2021_arq21.txt",  // Pergunta 15: Ação Afirmativa
    "DADOS_2021/microdados2021_arq24.txt",  // Pergunta 18: Modalidade Ensino Médio
    "DADOS_2021/microdados2021_arq25.txt",  // Pergunta 19: Incentivo
    "DADOS_2021/microdados2021_arq27.txt",  // Pergunta 21: Família com Superior
    "DADOS_2021/microdados2021_arq28.txt",  // Pergunta 22: Livros
    "DADOS_2021/microdados2021_arq29.txt"   // Pergunta 23: Horas de Estudo
};
const int NUM_ARQUIVOS_PERGUNTAS = 7;
const char* CODIGO_GRUPO_ADS = "72"; // Código oficial para a área de ADS

#define MAX_LINE_LEN 256 // Tamanho máximo de segurança para uma linha lida do arquivo
#define NUM_RESULTS 15   // Tamanho do array que guarda os contadores dos resultados

/* MAPEAMENTO DOS CONTADORES DE RESULTADOS (ÍNDICES DO ARRAY):
 * 0: [Q1] Total de alunos em Ação Afirmativa
 * 1: [Q2] Total de MULHERES em Ação Afirmativa
 * 2: [Q3] Total de Mulheres em A.A. que fizeram Ensino Técnico (QE_I18, Resposta 'B')
 * 3: [Q4] Total de Mulheres em A.A. com incentivo dos PAIS (QE_I19, Resposta 'B')
 * 4: [Q5] Total de alunos em A.A. com família com curso superior (QE_I21, Resposta 'A')
 * -- [Q6] Distribuição de Livros Lidos --
 * 5: Resposta 'A' (Nenhum), 6: 'B', 7: 'C', 8: 'D', 9: 'E'
 * -- [Q7] Distribuição de Horas de Estudo --
 * 10: Resposta 'A' (Nenhuma), 11: 'B', 12: 'C', 13: 'D', 14: 'E'
 */

// Explicação: Função auxiliar que verifica se um código de curso está na lista de cursos de ADS.
int is_ads_course(const char* co_curso, char** ads_course_list, int count) {
    for (int i = 0; i < count; i++) { if (strcmp(co_curso, ads_course_list[i]) == 0) return 1; }
    return 0;
}

// Explicação: Esta é a função de análise. Ela é executada em PARALELO por TODOS os processos.
// Cada processo recebe um "bloco" de texto com dados já unificados e processa suas linhas.
void analisar_bloco(char* bloco, long* resultados_locais) {
    char* linha_saveptr;
    char* linha = strtok_r(bloco, "\n", &linha_saveptr);
    while (linha != NULL) {
        char* campos[10]; int i = 0;
        char* campo_saveptr;
        char* token = strtok_r(linha, ";", &campo_saveptr);
        while (token != NULL && i < 10) { campos[i++] = token; token = strtok_r(NULL, ";", &campo_saveptr); }

        if (i >= 9) { // Garante que a linha tem todos os campos esperados
            // Explicação: Atribui os campos a variáveis com nomes legíveis para clareza.
            // A ordem dos campos (campos[2], campos[3], etc.) vem da ordem em que unificamos as respostas.
            const char* sexo = campos[2];
            const char* acao_afirmativa = campos[3];
            const char* modalidade_ens_medio = campos[4];
            const char* incentivo = campos[5];
            const char* familia_superior = campos[6];
            const char* livros = campos[7];
            const char* horas_estudo = campos[8];

            int ehAcaoAfirmativa = (strcmp(acao_afirmativa, "\"A\"") != 0); // "Não" é a única resposta que não é A.A.
            int ehFeminino = (strcmp(sexo, "\"F\"") == 0);

            // --- Bloco de Perguntas com filtro de Ação Afirmativa ---
            if (ehAcaoAfirmativa) {
                // [Q1] Conta o aluno como parte do grupo de Ação Afirmativa.
                resultados_locais[0]++; 
                // [Q5] Dentro do grupo de A.A., verifica se a família tem curso superior ('A' = Sim).
                if (strcmp(familia_superior, "\"A\"") == 0) resultados_locais[4]++;
                
                // --- Sub-bloco: Perguntas para MULHERES DENTRO de A.A. ---
                if (ehFeminino) {
                    // [Q2] Conta a aluna como mulher em Ação Afirmativa.
                    resultados_locais[1]++; 
                    // [Q3] Dentro do grupo de mulheres em A.A., verifica se fez Ensino Técnico ('B').
                    if (strcmp(modalidade_ens_medio, "\"B\"") == 0) resultados_locais[2]++;
                    // [Q4] Dentro do grupo de mulheres em A.A., verifica se o incentivo foi dos Pais ('B').
                    if (strcmp(incentivo, "\"B\"") == 0) resultados_locais[3]++;
                }
            }
            // --- Bloco de Pergunta 6 (Livros Lidos) - Executa para TODOS os alunos ---
            if (strcmp(livros, "\"A\"") == 0) resultados_locais[5]++;
            else if (strcmp(livros, "\"B\"") == 0) resultados_locais[6]++;
            else if (strcmp(livros, "\"C\"") == 0) resultados_locais[7]++;
            else if (strcmp(livros, "\"D\"") == 0) resultados_locais[8]++;
            else if (strcmp(livros, "\"E\"") == 0) resultados_locais[9]++;
            
            // --- Bloco de Pergunta 7 (Horas de Estudo) - Executa para TODOS os alunos ---
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
    // Explicação: Inicializa o ambiente MPI.
    MPI_Init(&argc, &argv);
    int rank, n_procs;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank); // Cada processo descobre sua ID (0 a 3)
    MPI_Comm_size(MPI_COMM_WORLD, &n_procs); // Todos descobrem que há 4 processos no total
    long resultados_locais[NUM_RESULTS] = {0}; // Cada processo cria sua "prancheta" local zerada.

    // Explicação: Apenas o Gerente (rank 0) executa este bloco. Ele é responsável por todo o I/O.
    if (rank == 0) {
        // --- ETAPA 1: Mapear os Cursos de ADS ---
        printf("Mestre (Rank 0): Mapeando cursos de ADS...\n");
        FILE* fp_mapa = fopen(ARQUIVO_MAPA_CURSOS, "r");
        if (!fp_mapa) { fprintf(stderr, "Erro fatal: Falha ao abrir mapa de cursos.\n"); MPI_Abort(MPI_COMM_WORLD, 1); }
        
        char** ads_courses_list = NULL;
        int ads_courses_count = 0;
        char line[MAX_LINE_LEN];
        fgets(line, sizeof(line), fp_mapa); // Pula o cabeçalho
        while (fgets(line, sizeof(line), fp_mapa)) {
            char* line_copy = strdup(line); // VITAL: strtok é destrutivo, sempre usar em cópias.
            if(!line_copy) { MPI_Abort(MPI_COMM_WORLD, 1); }
            char* co_curso_str, *co_grupo_str;
            strtok(line_copy, ";"); co_curso_str = strtok(NULL, ";"); strtok(NULL, ";"); strtok(NULL, ";"); strtok(NULL, ";"); co_grupo_str = strtok(NULL, ";");
            if (co_grupo_str && co_curso_str && strcmp(co_grupo_str, CODIGO_GRUPO_ADS) == 0) {
                ads_courses_count++;
                ads_courses_list = realloc(ads_courses_list, ads_courses_count * sizeof(char*));
                ads_courses_list[ads_courses_count - 1] = strdup(co_curso_str);
            }
            free(line_copy);
        }
        fclose(fp_mapa);
        printf("Mestre (Rank 0): Mapeamento concluído. %d cursos de ADS encontrados. Unificando dados...\n", ads_courses_count);
        
        // --- ETAPA 2: Unificar os Dados das Perguntas usando o Mapa de Cursos ---
        FILE* readers[NUM_ARQUIVOS_PERGUNTAS];
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
            readers[i] = fopen(NOMES_ARQUIVOS_PERGUNTAS[i], "r");
            if (!readers[i]) { fprintf(stderr, "Erro fatal: Falha ao abrir o arquivo %s\n", NOMES_ARQUIVOS_PERGUNTAS[i]); MPI_Abort(MPI_COMM_WORLD, 1); }
            fgets(line, sizeof(line), readers[i]);
        }
        size_t buffer_size = 1024 * 1024; // Buffer inicial de 1MB para os dados unificados
        char* dados_unificados = malloc(buffer_size);
        size_t current_pos = 0;
        dados_unificados[0] = '\0';
        char linhas[NUM_ARQUIVOS_PERGUNTAS][MAX_LINE_LEN];

        // Explicação: Loop principal de I/O. Lê uma linha de cada um dos 7 arquivos de perguntas.
        while(fgets(linhas[0], sizeof(linhas[0]), readers[0]) != NULL) {
            int linha_valida = 1;
            for(int i = 1; i < NUM_ARQUIVOS_PERGUNTAS; i++) { if (fgets(linhas[i], sizeof(linhas[i]), readers[i]) == NULL) { linha_valida = 0; break; } }
            if (!linha_valida) break;

            char* curso_copy = strdup(linhas[0]); // VITAL: Cópia para não destruir a linha original.
            if (!curso_copy) { MPI_Abort(MPI_COMM_WORLD, 1); }
            char* ano = strtok(curso_copy, ";");
            char* curso = strtok(NULL, ";");

            // Explicação: A verificação principal! O curso desta linha está na nossa lista de ADS?
            if (curso && is_ads_course(curso, ads_courses_list, ads_courses_count)) {
                char linha_combinada[MAX_LINE_LEN * 2];
                sprintf(linha_combinada, "%s;%s", ano, curso); // Inicia a super-linha com ano e curso
                
                // Explicação: Extrai apenas a resposta de cada uma das 7 linhas e concatena na super-linha.
                for(int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) {
                    char* resp_copy = strdup(linhas[i]);
                    if (!resp_copy) { MPI_Abort(MPI_COMM_WORLD, 1); }
                    strtok(resp_copy, ";"); strtok(NULL, ";"); 
                    char* resposta = strtok(NULL, "\n\r");
                    strcat(linha_combinada, ";");
                    if (resposta) strcat(linha_combinada, resposta);
                    free(resp_copy);
                }
                strcat(linha_combinada, "\n"); // Adiciona a quebra de linha no final

                // Explicação: Adiciona a super-linha completa ao buffer gigante.
                if (current_pos + strlen(linha_combinada) + 1 > buffer_size) {
                    buffer_size *= 2; // Dobra o tamanho do buffer se estiver cheio
                    dados_unificados = realloc(dados_unificados, buffer_size);
                }
                strcpy(dados_unificados + current_pos, linha_combinada);
                current_pos += strlen(linha_combinada);
            }
            free(curso_copy);
        }
        for (int i = 0; i < NUM_ARQUIVOS_PERGUNTAS; i++) fclose(readers[i]);
        for (int i = 0; i < ads_courses_count; i++) free(ads_courses_list[i]);
        free(ads_courses_list);
        
        // --- ETAPA 3: Distribuir o Trabalho ---
        printf("Mestre (Rank 0): Leitura concluída. Total de %zu bytes para TADS. Distribuindo trabalho...\n", current_pos);
        long chunk_size = current_pos > 0 ? current_pos / n_procs : 0;
        for (int i = 1; i < n_procs; i++) { // Envia para cada trabalhador (rank 1, 2, 3)
            long start = i * chunk_size;
            long size_to_send = (i == n_procs - 1) ? (current_pos - start) : chunk_size;
            MPI_Send(&size_to_send, 1, MPI_LONG, i, 0, MPI_COMM_WORLD);
            if (size_to_send > 0) MPI_Send(dados_unificados + start, size_to_send, MPI_CHAR, i, 0, MPI_COMM_WORLD);
        }
        if(chunk_size > 0) { // O Gerente processa sua própria parte
            char* my_bloco = malloc(chunk_size + 1);
            strncpy(my_bloco, dados_unificados, chunk_size);
            my_bloco[chunk_size] = '\0';
            analisar_bloco(my_bloco, resultados_locais);
            free(my_bloco);
        }
        free(dados_unificados);
    } 
    // Explicação: Trabalhadores (rank 1, 2, 3) executam este bloco. Eles esperam para receber os dados.
    else {
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

    // --- ETAPA 4: Agregação dos Resultados ---
    // Explicação: Todos os processos enviam suas pranchetas locais (`resultados_locais`). O MPI as SOMA
    // e entrega o resultado final na prancheta do Gerente (`resultados_globais`).
    long resultados_globais[NUM_RESULTS] = {0};
    MPI_Reduce(resultados_locais, resultados_globais, NUM_RESULTS, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // --- ETAPA 5: Impressão do Relatório Final ---
    // Explicação: Apenas o Gerente (rank 0), que tem os resultados finais, imprime o relatório.
    if (rank == 0) {
        long totalAfirmativa = resultados_globais[0];
        long mulheresAfirmativa = resultados_globais[1];
        long mulheresTecnico = resultados_globais[2];
        
        printf("\n--- RESULTADOS DA ANÁLISE ENADE 2021 (TADS) ---\n\n");
        // [Q1]
        printf("1. Total de alunos que ingressaram por Ações Afirmativas: %ld\n", totalAfirmativa);
        // [Q2]
        printf("2. Porcentagem de mulheres dentre os que ingressaram por Ações Afirmativas: %.2f%%\n", totalAfirmativa > 0 ? (double)mulheresAfirmativa / totalAfirmativa * 100.0 : 0);
        // [Q3]
        printf("3. Dentre as mulheres de A.A., porcentagem que concluiu Ensino Técnico: %.2f%%\n", mulheresAfirmativa > 0 ? (double)mulheresTecnico / mulheresAfirmativa * 100.0 : 0);
        // [Q4]
        printf("4. Dentre as mulheres de A.A., total que recebeu maior incentivo dos Pais: %ld\n", resultados_globais[3]);
        // [Q5]
        printf("5. Dentre os alunos de A.A., total com familiares com curso superior: %ld\n", resultados_globais[4]);
        
        // [Q6]
        printf("\n6. Quantos livros os alunos de TADS leram no ano?\n");
        printf("   - Nenhum (A):.................. %ld\n", resultados_globais[5]);
        printf("   - Um ou dois (B):.............. %ld\n", resultados_globais[6]);
        printf("   - Três a cinco (C):............ %ld\n", resultados_globais[7]);
        printf("   - Seis a oito (D):............. %ld\n", resultados_globais[8]);
        printf("   - Mais de oito (E):............ %ld\n", resultados_globais[9]);

        // [Q7]
        printf("\n7. Quantas horas semanais os alunos de TADS se dedicaram aos estudos?\n");
        printf("   - Nenhuma (A):................. %ld\n", resultados_globais[10]);
        printf("   - De uma a três (B):........... %ld\n", resultados_globais[11]);
        printf("   - De quatro a sete (C):........ %ld\n", resultados_globais[12]);
        printf("   - De oito a doze (D):.......... %ld\n", resultados_globais[13]);
        printf("   - Mais de doze (E):............ %ld\n", resultados_globais[14]);
    }
    
    // Explicação: Finaliza o ambiente MPI.
    MPI_Finalize();
    return 0;
}
