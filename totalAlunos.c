#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Definindo constantes para facilitar a manutenção
#define NOME_ARQUIVO "DADOS/microdados2021_arq1.txt"
#define TAMANHO_BUFFER 1024 // Um buffer de 1KB para cada linha deve ser suficiente
#define COLUNA_ALVO 6       // A sexta coluna (CO_GRUPO)
#define VALOR_ALVO "72"     // O valor que estamos procurando

int main() {
    FILE *arquivo;
    char linha[TAMANHO_BUFFER];
    int contador_72 = 0;
    long long numero_linha = 0;

    // 1. Abrir o arquivo para leitura
    arquivo = fopen(NOME_ARQUIVO, "r");

    // Verificar se o arquivo foi aberto com sucesso
    if (arquivo == NULL) {
        perror("Erro ao abrir o arquivo");
        return 1; // Retorna 1 para indicar um erro
    }

    // 2. Ler e descartar a primeira linha (cabeçalho)
    if (fgets(linha, sizeof(linha), arquivo) == NULL) {
        printf("Arquivo está vazio ou ocorreu um erro de leitura.\n");
        fclose(arquivo);
        return 0; // Arquivo vazio, terminamos
    }
    numero_linha++;

    // 3. Ler o arquivo linha por linha até o final
    while (fgets(linha, sizeof(linha), arquivo) != NULL) {
        numero_linha++;
        char *token;
        int coluna_atual = 1;

        // A função strtok modifica a string original.
        // A primeira chamada a strtok usa a string 'linha'
        token = strtok(linha, ";");

        // Loop para encontrar a coluna de interesse
        while (token != NULL) {
            if (coluna_atual == COLUNA_ALVO) {
                // 4. Comparar o valor da sexta coluna com "72"
                if (strcmp(token, VALOR_ALVO) == 0) {
                    contador_72++;
                }
                break; // Encontramos a coluna, podemos parar de processar esta linha
            }
            
            // As chamadas seguintes a strtok usam NULL para continuar de onde parou
            token = strtok(NULL, ";");
            coluna_atual++;
        }
    }

    // 5. Fechar o arquivo para liberar os recursos
    fclose(arquivo);

    // 6. Imprimir o resultado final
    printf("O arquivo '%s' foi processado.\n", NOME_ARQUIVO);
    printf("Total de linhas lidas: %lld\n", numero_linha);
    printf("O numero '%s' apareceu um total de %d vezes na sexta coluna.\n", VALOR_ALVO, contador_72);

    return 0; // Retorna 0 para indicar sucesso
}