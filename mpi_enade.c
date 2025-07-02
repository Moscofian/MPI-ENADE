#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TAMANHO_MAX_LINHA 2048
#define CODIGO_GRUPO_ADS 72

// Estrutura atualizada para incluir contadores de respostas nulas/inválidas.
// Todos os nomes de variáveis foram traduzidos para o português.
typedef struct {
    long long total_alunos;
    long long alunas_sexo_feminino;
    long long alunos_ensino_tecnico;
    long long total_acao_afirmativa;
    // Contadores de Incentivo
    long long incentivo_pais, incentivo_familia, incentivo_professores, incentivo_amigos,
              incentivo_lider_religioso, incentivo_outros, incentivo_nenhum, incentivo_nulo;
    // Contadores de Família Graduada
    long long familia_graduada_sim, familia_graduada_nao, familia_graduada_nulo;
    // Contadores de Livros
    long long livros_nenhum, livros_um_a_dois, livros_tres_a_cinco, livros_seis_a_oito, 
              livros_mais_de_oito, livros_nulo;
    // Contadores de Horas de Estudo
    long long horas_estudo_nenhuma, horas_estudo_uma_a_tres, horas_estudo_quatro_a_sete, 
              horas_estudo_oito_a_doze, horas_estudo_mais_de_doze, horas_estudo_nulo;
} Resultados;

// Função para verificar se um curso já existe na lista para evitar duplicatas.
int curso_esta_na_lista(int codigo_curso, const int* lista, int tamanho) {
    for (int i = 0; i < tamanho; i++) {
        if (lista[i] == codigo_curso) return 1; // Encontrado
    }
    return 0; // Não encontrado
}

// Função para verificar se um código de curso pertence à lista final de cursos de ADS
int eh_curso_de_ads(int codigo_curso, const int* cursos_ads, int num_cursos_ads) {
    for (int i = 0; i < num_cursos_ads; i++) {
        if (cursos_ads[i] == codigo_curso) return 1;
    }
    return 0;
}

// Processa um único arquivo de dados e atualiza os contadores locais
void processar_arquivo_de_dados(const char* nome_arquivo, int rank_processo, int num_processos, const int* cursos_ads, int num_cursos_ads, Resultados* resultados_locais) {
    FILE* arquivo = fopen(nome_arquivo, "r");
    if (!arquivo) {
        if (rank_processo == 0) fprintf(stderr, "Aviso: Não foi possível abrir %s.\n", nome_arquivo);
        return;
    }

    char linha[TAMANHO_MAX_LINHA];
    long long numero_linha = 0;
    fgets(linha, sizeof(linha), arquivo); // Pula cabeçalho

    while (fgets(linha, sizeof(linha), arquivo)) {
        if (numero_linha % num_processos == rank_processo) {
            int ano = 0, codigo_curso = 0;
            char resposta_str[2] = {0};

            sscanf(linha, "%d;%d;\"%1s\"", &ano, &codigo_curso, resposta_str);
            
            if (eh_curso_de_ads(codigo_curso, cursos_ads, num_cursos_ads)) {
                if (strcmp(nome_arquivo, "DADOS/microdados2021_arq5.txt") == 0) {
                    resultados_locais->total_alunos++;
                    if (resposta_str[0] == 'F') resultados_locais->alunas_sexo_feminino++;
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq24.txt") == 0) {
                    if (resposta_str[0] == 'B') resultados_locais->alunos_ensino_tecnico++;
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq21.txt") == 0) {
                    if (resposta_str[0] != 'A' && resposta_str[0] != ' ' && resposta_str[0] != '.') resultados_locais->total_acao_afirmativa++;
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq25.txt") == 0) {
                    switch (resposta_str[0]) {
                        case 'A': resultados_locais->incentivo_nenhum++; break; case 'B': resultados_locais->incentivo_pais++; break;
                        case 'C': resultados_locais->incentivo_familia++; break; case 'D': resultados_locais->incentivo_professores++; break;
                        case 'E': resultados_locais->incentivo_lider_religioso++; break; case 'F': resultados_locais->incentivo_amigos++; break;
                        case 'G': resultados_locais->incentivo_outros++; break; default: resultados_locais->incentivo_nulo++; break;
                    }
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq27.txt") == 0) {
                    switch (resposta_str[0]) {
                        case 'A': resultados_locais->familia_graduada_sim++; break; case 'B': resultados_locais->familia_graduada_nao++; break;
                        default: resultados_locais->familia_graduada_nulo++; break;
                    }
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq28.txt") == 0) {
                    switch (resposta_str[0]) {
                        case 'A': resultados_locais->livros_nenhum++; break; case 'B': resultados_locais->livros_um_a_dois++; break;
                        case 'C': resultados_locais->livros_tres_a_cinco++; break; case 'D': resultados_locais->livros_seis_a_oito++; break;
                        case 'E': resultados_locais->livros_mais_de_oito++; break; default: resultados_locais->livros_nulo++; break;
                    }
                } else if (strcmp(nome_arquivo, "DADOS/microdados2021_arq29.txt") == 0) {
                    switch (resposta_str[0]) {
                        case 'A': resultados_locais->horas_estudo_nenhuma++; break; case 'B': resultados_locais->horas_estudo_uma_a_tres++; break;
                        case 'C': resultados_locais->horas_estudo_quatro_a_sete++; break; case 'D': resultados_locais->horas_estudo_oito_a_doze++; break;
                        case 'E': resultados_locais->horas_estudo_mais_de_doze++; break; default: resultados_locais->horas_estudo_nulo++; break;
                    }
                }
            }
        }
        numero_linha++;
    }
    fclose(arquivo);
}

// Função de impressão dos resultados finais
void imprimir_resultados_finais(int num_cursos_ads, const Resultados* resultados_finais) {
    printf("\n===================================================\n");
    printf("   Análise dos Microdados do ENADE para ADS\n");
    printf("===================================================\n\n");
    
    printf("DETECÇÃO INICIAL:\n");
    printf("- Cursos de ADS (CO_GRUPO %d) capturados: %d\n", CODIGO_GRUPO_ADS, num_cursos_ads);
    printf("- Total de alunos de ADS detectados (baseado no arq5): %lld\n\n", resultados_finais->total_alunos);
    printf("---------------------------------------------------\n\n");

    if (resultados_finais->total_alunos == 0) {
        printf("Nenhum estudante do curso de ADS foi encontrado nos dados para análise.\n");
        return;
    }
    long long contagem_base_total = resultados_finais->total_alunos;

    printf("1. Qual é a porcentagem de estudante do sexo Feminino?\n");
    double percentual_feminino = (contagem_base_total > 0) ? (double)resultados_finais->alunas_sexo_feminino * 100.0 / contagem_base_total : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", resultados_finais->alunas_sexo_feminino, percentual_feminino);

    printf("2. Qual é a porcentagem de estudantes que cursaram o ensino técnico no ensino médio?\n");
    double percentual_ensino_tecnico = (contagem_base_total > 0) ? (double)resultados_finais->alunos_ensino_tecnico * 100.0 / contagem_base_total : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", resultados_finais->alunos_ensino_tecnico, percentual_ensino_tecnico);

    printf("3. Qual é o percentual de alunos provenientes de ações afirmativas?\n");
    double percentual_acao_afirmativa = (contagem_base_total > 0) ? (double)resultados_finais->total_acao_afirmativa * 100.0 / contagem_base_total : 0;
    printf("   Resposta: %lld estudantes (%.2f%% do total).\n\n", resultados_finais->total_acao_afirmativa, percentual_acao_afirmativa);
    
    printf("4. Dos estudantes, quem deu incentivo para este estudante cursar o ADS?\n");
    long long total_incentivo_valido = resultados_finais->incentivo_pais + resultados_finais->incentivo_familia + resultados_finais->incentivo_professores + resultados_finais->incentivo_amigos + resultados_finais->incentivo_lider_religioso + resultados_finais->incentivo_outros + resultados_finais->incentivo_nenhum;
    if (total_incentivo_valido > 0) {
        printf("   - Pais:                 %lld (%.2f%%)\n", resultados_finais->incentivo_pais, (double)resultados_finais->incentivo_pais * 100.0 / total_incentivo_valido);
        printf("   - Outros familiares:    %lld (%.2f%%)\n", resultados_finais->incentivo_familia, (double)resultados_finais->incentivo_familia * 100.0 / total_incentivo_valido);
        printf("   - Professores:          %lld (%.2f%%)\n", resultados_finais->incentivo_professores, (double)resultados_finais->incentivo_professores * 100.0 / total_incentivo_valido);
        printf("   - Colegas/Amigos:       %lld (%.2f%%)\n", resultados_finais->incentivo_amigos, (double)resultados_finais->incentivo_amigos * 100.0 / total_incentivo_valido);
        printf("   - Líder religioso:      %lld (%.2f%%)\n", resultados_finais->incentivo_lider_religioso, (double)resultados_finais->incentivo_lider_religioso * 100.0 / total_incentivo_valido);
        printf("   - Outras pessoas:       %lld (%.2f%%)\n", resultados_finais->incentivo_outros, (double)resultados_finais->incentivo_outros * 100.0 / total_incentivo_valido);
        printf("   - Ninguém:              %lld (%.2f%%)\n", resultados_finais->incentivo_nenhum, (double)resultados_finais->incentivo_nenhum * 100.0 / total_incentivo_valido);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", resultados_finais->incentivo_nulo);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }

    printf("5. Quantos estudantes apresentaram familiares com o curso superior concluído?\n");
    long long total_familia_valido = resultados_finais->familia_graduada_sim + resultados_finais->familia_graduada_nao;
     if (total_familia_valido > 0) {
        printf("   - Sim: %lld (%.2f%%)\n", resultados_finais->familia_graduada_sim, (double)resultados_finais->familia_graduada_sim * 100.0 / total_familia_valido);
        printf("   - Não: %lld (%.2f%%)\n", resultados_finais->familia_graduada_nao, (double)resultados_finais->familia_graduada_nao * 100.0 / total_familia_valido);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", resultados_finais->familia_graduada_nulo);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }

    printf("6. Quantos livros os alunos leram no ano do ENADE?\n");
    long long total_livros_valido = resultados_finais->livros_nenhum + resultados_finais->livros_um_a_dois + resultados_finais->livros_tres_a_cinco + resultados_finais->livros_seis_a_oito + resultados_finais->livros_mais_de_oito;
    if (total_livros_valido > 0) {
       printf("   - Nenhum:        %lld (%.2f%%)\n", resultados_finais->livros_nenhum, (double)resultados_finais->livros_nenhum * 100.0 / total_livros_valido);
       printf("   - 1 ou 2:        %lld (%.2f%%)\n", resultados_finais->livros_um_a_dois, (double)resultados_finais->livros_um_a_dois * 100.0 / total_livros_valido);
       printf("   - 3 a 5:         %lld (%.2f%%)\n", resultados_finais->livros_tres_a_cinco, (double)resultados_finais->livros_tres_a_cinco * 100.0 / total_livros_valido);
       printf("   - 6 a 8:         %lld (%.2f%%)\n", resultados_finais->livros_seis_a_oito, (double)resultados_finais->livros_seis_a_oito * 100.0 / total_livros_valido);
       printf("   - Mais de 8:     %lld (%.2f%%)\n", resultados_finais->livros_mais_de_oito, (double)resultados_finais->livros_mais_de_oito * 100.0 / total_livros_valido);
       printf("   - Respostas Nulas/Inválidas: %lld\n\n", resultados_finais->livros_nulo);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }
    
    printf("7. Quantas horas na semana os estudantes se dedicaram aos estudos?\n");
    long long total_estudo_valido = resultados_finais->horas_estudo_nenhuma + resultados_finais->horas_estudo_uma_a_tres + resultados_finais->horas_estudo_quatro_a_sete + resultados_finais->horas_estudo_oito_a_doze + resultados_finais->horas_estudo_mais_de_doze;
    if (total_estudo_valido > 0) {
        printf("   - Nenhuma (só aulas): %lld (%.2f%%)\n", resultados_finais->horas_estudo_nenhuma, (double)resultados_finais->horas_estudo_nenhuma * 100.0 / total_estudo_valido);
        printf("   - 1 a 3 horas:        %lld (%.2f%%)\n", resultados_finais->horas_estudo_uma_a_tres, (double)resultados_finais->horas_estudo_uma_a_tres * 100.0 / total_estudo_valido);
        printf("   - 4 a 7 horas:        %lld (%.2f%%)\n", resultados_finais->horas_estudo_quatro_a_sete, (double)resultados_finais->horas_estudo_quatro_a_sete * 100.0 / total_estudo_valido);
        printf("   - 8 a 12 horas:       %lld (%.2f%%)\n", resultados_finais->horas_estudo_oito_a_doze, (double)resultados_finais->horas_estudo_oito_a_doze * 100.0 / total_estudo_valido);
        printf("   - Mais de 12 horas:   %lld (%.2f%%)\n", resultados_finais->horas_estudo_mais_de_doze, (double)resultados_finais->horas_estudo_mais_de_doze * 100.0 / total_estudo_valido);
        printf("   - Respostas Nulas/Inválidas: %lld\n\n", resultados_finais->horas_estudo_nulo);
    } else { printf("   Nenhum dado encontrado para esta questão.\n\n"); }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int num_processos, rank_processo;
    MPI_Comm_size(MPI_COMM_WORLD, &num_processos);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank_processo);
    
    double tempo_inicio;

    if (rank_processo == 0) {
        printf("Análise iniciada com %d processos.\n", num_processos);
        tempo_inicio = MPI_Wtime();
    }
    
    int num_cursos_ads = 0;
    int* cursos_ads = NULL; 

    if (rank_processo == 0) {
        int capacidade = 0; 
        const char* caminho_arq1 = "DADOS/microdados2021_arq1.txt";
        printf("Processo 0: Lendo a lista de cursos de ADS de '%s'...\n", caminho_arq1);
        
        FILE* arquivo = fopen(caminho_arq1, "r");
        if (!arquivo) {
            fprintf(stderr, "Erro fatal: não foi possível abrir '%s'.\n", caminho_arq1);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        char linha[TAMANHO_MAX_LINHA];
        fgets(linha, sizeof(linha), arquivo);

        while (fgets(linha, sizeof(linha), arquivo)) {
            int codigo_curso = 0, codigo_grupo = 0;
            
            // Lê os números puros do arquivo arq1.txt, pulando as colunas desnecessárias.
            int itens_lidos = sscanf(linha, "%*[^;];%d;%*[^;];%*[^;];%*[^;];%d", &codigo_curso, &codigo_grupo);
            
            if (itens_lidos >= 2 && codigo_grupo == CODIGO_GRUPO_ADS) {
                // Adiciona o curso somente se ele for ÚNICO.
                if (!curso_esta_na_lista(codigo_curso, cursos_ads, num_cursos_ads)) {
                    if (num_cursos_ads == capacidade) {
                        int nova_capacidade = (capacidade == 0) ? 100 : capacidade * 2;
                        int* ponteiro_temp = realloc(cursos_ads, nova_capacidade * sizeof(int));
                        if (!ponteiro_temp) {
                            fprintf(stderr, "Erro fatal: falha ao alocar memória.\n");
                            free(cursos_ads);
                            MPI_Abort(MPI_COMM_WORLD, 1);
                        }
                        cursos_ads = ponteiro_temp;
                        capacidade = nova_capacidade;
                    }
                    cursos_ads[num_cursos_ads++] = codigo_curso;
                }
            }
        }
        fclose(arquivo);
        printf("Processo 0: Encontrou %d cursos únicos. Distribuindo para os outros processos...\n", num_cursos_ads);
    }

    MPI_Bcast(&num_cursos_ads, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank_processo != 0) {
        cursos_ads = (int*)malloc(num_cursos_ads * sizeof(int));
        if (cursos_ads == NULL && num_cursos_ads > 0) {
             fprintf(stderr, "Processo %d: falha ao alocar memória.\n", rank_processo);
             MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    MPI_Bcast(cursos_ads, num_cursos_ads, MPI_INT, 0, MPI_COMM_WORLD);
    
    MPI_Barrier(MPI_COMM_WORLD); 
    if (rank_processo == 0) printf("\nIniciando a análise paralela dos arquivos de dados...\n\n");

    Resultados resultados_locais = {0}; // Inicializa todos os campos com 0
    const char* arquivos_de_dados[] = {
        "DADOS/microdados2021_arq5.txt", "DADOS/microdados2021_arq21.txt", "DADOS/microdados2021_arq24.txt",
        "DADOS/microdados2021_arq25.txt", "DADOS/microdados2021_arq27.txt", "DADOS/microdados2021_arq28.txt",
        "DADOS/microdados2021_arq29.txt"
    };
    int num_arquivos_de_dados = sizeof(arquivos_de_dados) / sizeof(arquivos_de_dados[0]);

    for (int i = 0; i < num_arquivos_de_dados; ++i) {
        if (rank_processo == 0) printf("Analisando: %s...\n", arquivos_de_dados[i]);
        processar_arquivo_de_dados(arquivos_de_dados[i], rank_processo, num_processos, cursos_ads, num_cursos_ads, &resultados_locais);
        MPI_Barrier(MPI_COMM_WORLD); 
    }

    if (rank_processo == 0) printf("\nAnálise paralela concluída. Agregando resultados...\n");

    Resultados resultados_finais = {0};
    int num_elementos_struct = sizeof(Resultados) / sizeof(long long);
    MPI_Reduce(&resultados_locais, &resultados_finais, num_elementos_struct, MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank_processo == 0) {
        double tempo_fim = MPI_Wtime();
        imprimir_resultados_finais(num_cursos_ads, &resultados_finais);
        printf("---------------------------------------------------\n");
        printf("Análise concluída em %.4f segundos.\n", tempo_fim - tempo_inicio);
    }
    
    free(cursos_ads);
    MPI_Finalize();
    return 0;
}
