// Arquivo: analisador_enade_hipotetico_pt.c
// Descrição: Versão HIPOTÉTICA do analisador, em português, assumindo que cada
//            arquivo de dados contém uma coluna "ID_ALUNO" para correlacionar os dados.
//            Este código demonstra como as perguntas complexas seriam respondidas.
//            Projetado para ser executado com EXATAMENTE 4 processos.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <ctype.h>

// =================================================================================
// --- SEÇÃO DE CONFIGURAÇÃO ---
// =================================================================================
const char* ARQUIVOS_QUESTOES[] = { "microdados2021_arq21.txt", "microdados2021_arq5.txt", "microdados2021_arq24.txt", "microdados2021_arq25.txt", "microdados2021_arq27.txt", "microdados2021_arq28.txt", "microdados2021_arq29.txt" };
enum IndiceQuestao { ACAO_AFIRMATIVA = 0, SEXO = 1, TIPO_EM = 2, INCENTIVO = 3, ESCOLARIDADE_FAM = 4, LIVROS = 5, HORAS_ESTUDO = 6 };
const int NUM_ARQUIVOS_QUESTOES = 7;
const char* ARQUIVO_MAPA_CURSOS = "microdados2021_arq1.txt";
const long CODIGO_GRUPO_TADS = 72;
#define TAM_MAX_LINHA 512
#define TAM_CODIGO_CURSO 10

// =================================================================================
// --- ESTRUTURAS DE DADOS ---
// =================================================================================

// Struct para armazenar todas as respostas de um único aluno.
typedef struct {
    char acao_afirmativa[2]; char sexo[2]; char modalidade_em[2];
    char incentivo[2]; char escolaridade_fam[2]; char livros[2]; char horas_estudo[2];
} RespostasAluno;

// Struct para unificar dados na memória do Mestre
typedef struct {
    long id_aluno;
    RespostasAluno respostas;
} DadosAlunoUnificado;

// Struct completa para os resultados de TODAS as perguntas.
typedef struct {
    long total_alunos_tads;
    long alunos_aa;
    long mulheres_aa;
    long mulheres_aa_tecnico;
    long familiares_graduados_aa;
    long incentivo_pais, incentivo_parentes, incentivo_professores, incentivo_amigos, incentivo_religioso, incentivo_outros;
    long livros_nenhum, livros_1_a_2, livros_3_a_5, livros_6_a_8, livros_mais_de_8;
    long estudo_nenhuma, estudo_1_a_3, estudo_4_a_7, estudo_8_a_12, estudo_mais_de_12;
} ResultadosAnaliseCompleta;

// =================================================================================
// --- FUNÇÕES AUXILIARES ---
// =================================================================================
void limpar_aspas_e_espacos(char *str) { if (str == NULL || *str == '\0') return; char *inicio = str; while (isspace((unsigned char)*inicio) || *inicio == '"') inicio++; char *fim = str + strlen(str) - 1; while (fim > inicio && (isspace((unsigned char)*fim) || *fim == '"')) fim--; *(fim + 1) = '\0'; if (inicio != str) memmove(str, inicio, strlen(inicio) + 1); }
int eh_curso_tads(const char* codigo_curso, char (*lista_tads)[TAM_CODIGO_CURSO], int contagem) { for (int i = 0; i < contagem; i++) { if (strcmp(codigo_curso, lista_tads[i]) == 0) return 1; } return 0; }

void analisar_dados_aluno(const RespostasAluno* respostas, ResultadosAnaliseCompleta* resultados) {
    resultados->total_alunos_tads++;
    int eh_aa = (strcmp(respostas->acao_afirmativa, "A") != 0 && strcmp(respostas->acao_afirmativa, ".") != 0 && strlen(respostas->acao_afirmativa) > 0);
    int eh_mulher = (strcmp(respostas->sexo, "F") == 0);

    // Contabiliza livros e horas de estudo (perguntas simples)
    if (strcmp(respostas->livros, "A") == 0) resultados->livros_nenhum++; else if (strcmp(respostas->livros, "B") == 0) resultados->livros_1_a_2++; else if (strcmp(respostas->livros, "C") == 0) resultados->livros_3_a_5++; else if (strcmp(respostas->livros, "D") == 0) resultados->livros_6_a_8++; else if (strcmp(respostas->livros, "E") == 0) resultados->livros_mais_de_8++;
    if (strcmp(respostas->horas_estudo, "A") == 0) resultados->estudo_nenhuma++; else if (strcmp(respostas->horas_estudo, "B") == 0) resultados->estudo_1_a_3++; else if (strcmp(respostas->horas_estudo, "C") == 0) resultados->estudo_4_a_7++; else if (strcmp(respostas->horas_estudo, "D") == 0) resultados->estudo_8_a_12++; else if (strcmp(respostas->horas_estudo, "E") == 0) resultados->estudo_mais_de_12++;

    // Análises complexas que exigem cruzamento de dados
    if (eh_aa) {
        resultados->alunos_aa++;
        if (strcmp(respostas->escolaridade_fam, "A") != 0 && strcmp(respostas->escolaridade_fam, ".") != 0 && strlen(respostas->escolaridade_fam) > 0) { resultados->familiares_graduados_aa++; }
        if (eh_mulher) {
            resultados->mulheres_aa++;
            if (strcmp(respostas->modalidade_em, "B") == 0) { resultados->mulheres_aa_tecnico++; }
            if (strcmp(respostas->incentivo, "B") == 0) resultados->incentivo_pais++; else if (strcmp(respostas->incentivo, "C") == 0) resultados->incentivo_parentes++; else if (strcmp(respostas->incentivo, "D") == 0) resultados->incentivo_professores++; else if (strcmp(respostas->incentivo, "F") == 0) resultados->incentivo_amigos++; else if (strcmp(respostas->incentivo, "E") == 0) resultados->incentivo_religioso++; else if (strcmp(respostas->incentivo, "G") == 0) resultados->incentivo_outros++;
        }
    }
}

// =================================================================================
// --- FUNÇÃO PRINCIPAL ---
// =================================================================================
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int meu_rank, num_processos;
    MPI_Comm_rank(MPI_COMM_WORLD, &meu_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processos);

    // Garante que o programa seja executado com exatamente 4 processos
    if (num_processos != 4) {
        if (meu_rank == 0) {
            fprintf(stderr, "Erro: Este programa foi projetado para ser executado com exatamente 4 processos.\n");
            fprintf(stderr, "Por favor, execute com: mpiexec -n 4 %s\n", argv[0]);
        }
        MPI_Finalize();
        return 1;
    }

    ResultadosAnaliseCompleta resultados_locais = {0};
    RespostasAluno* meu_bloco_trabalho = NULL;
    int tamanho_meu_bloco = 0;

    // --- MESTRE (RANK 0): FASE DE UNIFICAÇÃO E DISTRIBUIÇÃO ---
    if (meu_rank == 0) {
        // ETAPA 1.1: Mapear cursos de TADS (código omitido por brevidade, seria igual ao do outro programa)
        char (*lista_codigos_tads)[TAM_CODIGO_CURSO] = NULL; int contagem_cursos_tads = 0;
        // ...
        
        // ETAPA 1.2: Unificar dados dos alunos de TADS na memória
        printf("Mestre (Rank 0): Unificando dados de TADS. Isso pode levar um tempo...\n");
        DadosAlunoUnificado* base_dados_unificada = NULL;
        long tamanho_base_dados = 0;

        for (int i = 0; i < NUM_ARQUIVOS_QUESTOES; i++) {
            FILE* fp = fopen(ARQUIVOS_QUESTOES[i], "r");
            if (!fp) { fprintf(stderr, "Erro ao abrir %s\n", ARQUIVOS_QUESTOES[i]); continue; }
            char linha[TAM_MAX_LINHA];
            fgets(linha, sizeof(linha), fp);

            while (fgets(linha, sizeof(linha), fp)) {
                char* copia_linha = strdup(linha); char* saveptr;
                strtok_r(copia_linha, ";", &saveptr); // NU_ANO
                char* str_id = strtok_r(NULL, ";", &saveptr); // **ASSUMINDO ID_ALUNO AQUI**
                char* str_curso = strtok_r(NULL, ";", &saveptr);
                char* str_resposta = strtok_r(NULL, "\r\n", &saveptr);
                if (!str_id || !str_curso || !str_resposta) { free(copia_linha); continue; }
                
                limpar_aspas_e_espacos(str_curso);
                // Aqui, a verificação 'eh_curso_tads' seria crucial. Omitida para simplificar o exemplo.
                // if (eh_curso_tads(str_curso, lista_codigos_tads, contagem_cursos_tads)) {
                    long id_atual = atol(str_id);
                    limpar_aspas_e_espacos(str_resposta);

                    // Procura pelo aluno no nosso "banco de dados" em memória
                    int indice_encontrado = -1;
                    for (int j = 0; j < tamanho_base_dados; j++) { if (base_dados_unificada[j].id_aluno == id_atual) { indice_encontrado = j; break; } }

                    // Se for um aluno novo, cria um registro para ele
                    if (indice_encontrado == -1) {
                        tamanho_base_dados++;
                        base_dados_unificada = realloc(base_dados_unificada, tamanho_base_dados * sizeof(DadosAlunoUnificado));
                        indice_encontrado = tamanho_base_dados - 1;
                        base_dados_unificada[indice_encontrado].id_aluno = id_atual;
                        memset(&base_dados_unificada[indice_encontrado].respostas, 0, sizeof(RespostasAluno));
                    }
                    
                    // Armazena a resposta no campo correto da struct do aluno
                    char* campo_alvo = NULL;
                    switch(i) {
                        case ACAO_AFIRMATIVA:   campo_alvo = base_dados_unificada[indice_encontrado].respostas.acao_afirmativa; break;
                        case SEXO:              campo_alvo = base_dados_unificada[indice_encontrado].respostas.sexo; break;
                        case TIPO_EM:           campo_alvo = base_dados_unificada[indice_encontrado].respostas.modalidade_em; break;
                        case INCENTIVO:         campo_alvo = base_dados_unificada[indice_encontrado].respostas.incentivo; break;
                        case ESCOLARIDADE_FAM:  campo_alvo = base_dados_unificada[indice_encontrado].respostas.escolaridade_fam; break;
                        case LIVROS:            campo_alvo = base_dados_unificada[indice_encontrado].respostas.livros; break;
                        case HORAS_ESTUDO:      campo_alvo = base_dados_unificada[indice_encontrado].respostas.horas_estudo; break;
                    }
                    if (campo_alvo) strncpy(campo_alvo, str_resposta, 2);
                // }
                free(copia_linha);
            }
            fclose(fp);
        }
        printf("Mestre (Rank 0): Unificação concluída. %ld alunos de TADS encontrados.\n", tamanho_base_dados);

        // ETAPA 1.3: Distribuir o trabalho (os dossiês dos alunos)
        int bloco_base = tamanho_base_dados / num_processos;
        int resto = tamanho_base_dados % num_processos;
        int pos_atual = 0;
        for (int i = 1; i < num_processos; i++) {
            int tamanho_bloco_enviar = bloco_base + (i < resto ? 1 : 0);
            MPI_Send(&tamanho_bloco_enviar, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&base_dados_unificada[pos_atual].respostas, tamanho_bloco_enviar * sizeof(RespostasAluno), MPI_BYTE, i, 0, MPI_COMM_WORLD);
            pos_atual += tamanho_bloco_enviar;
        }
        tamanho_meu_bloco = bloco_base + (0 < resto ? 1 : 0);
        meu_bloco_trabalho = malloc(tamanho_meu_bloco * sizeof(RespostasAluno));
        memcpy(meu_bloco_trabalho, &base_dados_unificada[pos_atual].respostas, tamanho_meu_bloco * sizeof(RespostasAluno));
        free(base_dados_unificada);
        free(lista_codigos_tads);
    } 
    // --- TRABALHADORES (RANKS 1, 2, 3): RECEBEM SEU BLOCO DE TRABALHO ---
    else {
        MPI_Recv(&tamanho_meu_bloco, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        meu_bloco_trabalho = malloc(tamanho_meu_bloco * sizeof(RespostasAluno));
        MPI_Recv(meu_bloco_trabalho, tamanho_meu_bloco * sizeof(RespostasAluno), MPI_BYTE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    
    // --- TODOS OS PROCESSOS: FASE DE ANÁLISE PARALELA ---
    for (int i = 0; i < tamanho_meu_bloco; i++) {
        analisar_dados_aluno(&meu_bloco_trabalho[i], &resultados_locais);
    }
    free(meu_bloco_trabalho);
    
    // --- AGREGAÇÃO E RESULTADO FINAL ---
    ResultadosAnaliseCompleta resultados_globais = {0};
    MPI_Reduce(&resultados_locais, &resultados_globais, sizeof(ResultadosAnaliseCompleta)/sizeof(long), MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    // --- MESTRE (RANK 0): IMPRIME O RELATÓRIO FINAL COMPLETO ---
    if (meu_rank == 0) {
        printf("\n--- RESULTADOS (HIPOTÉTICOS) DA ANÁLISE COMPLETA ENADE 2021 (CURSO: TADS) ---\n\n");
        printf("Total de registros de alunos de TADS analisados: %ld\n\n", resultados_globais.total_alunos_tads);
        printf("1. Quantos alunos se matricularam por meio de ações afirmativas?\n   -> Total: %ld\n\n", resultados_globais.alunos_aa);
        printf("2. Qual é a porcentagem de estudantes do sexo Feminino que se matricularam por ações afirmativas?\n   -> Porcentagem: %.2f%% (%ld de %ld alunos de A.A.)\n\n", (resultados_globais.alunos_aa > 0) ? (double)resultados_globais.mulheres_aa / resultados_globais.alunos_aa * 100.0 : 0.0, resultados_globais.mulheres_aa, resultados_globais.alunos_aa);
        printf("3. Qual é a porcentagem de estudantes (sexo Feminino e de A.A.) que cursaram o ensino médio técnico?\n   -> Porcentagem: %.2f%% (%ld de %ld mulheres em A.A.)\n\n", (resultados_globais.mulheres_aa > 0) ? (double)resultados_globais.mulheres_aa_tecnico / resultados_globais.mulheres_aa * 100.0 : 0.0, resultados_globais.mulheres_aa_tecnico, resultados_globais.mulheres_aa);
        printf("4. Dos estudantes do sexo Feminino e de A.A., quem deu maior incentivo para cursar a graduação?\n   - Pais: %ld\n   - Outros familiares: %ld\n   ...\n\n", resultados_globais.incentivo_pais, resultados_globais.incentivo_parentes);
        printf("5. Dos estudantes de ações afirmativas, quantos apresentaram familiares com curso superior concluído?\n   -> Total: %ld\n\n", resultados_globais.familiares_graduados_aa);
        // Impressão das perguntas 6 e 7 (livros e horas de estudo) omitida por brevidade
        printf("-----------------------------------------------------------\n");
    }

    MPI_Finalize();
    return 0;
}
