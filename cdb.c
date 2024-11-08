#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define sizeOfAttribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
#define TABLE_MAX_PAGES 100
typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum
{
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum
{
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct
{
    char *buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef struct
{
    int id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

const int ID_SIZE = sizeOfAttribute(Row, id);
const int USERNAME_SIZE = sizeOfAttribute(Row, username);
const int EMAIL_SIZE = sizeOfAttribute(Row, email);
const int ID_OFFSET = 0;
const int USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const int EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const int ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const int PAGE_SIZE = 4096;
const int ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const int TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct
{
    StatementType type;
    Row rowToInsert;
} Statement;

typedef struct
{
    int numRows;
    void *pages[TABLE_MAX_PAGES];
} Table;

void serializeRow(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void *rowSlot(Table *table, int rowNum)
{
    int pageNum = rowNum / ROWS_PER_PAGE;
    void *page = table->pages[pageNum];
    if (page == NULL)
    {
        page = table->pages[pageNum] = malloc(PAGE_SIZE);
    }
    int rowOffset = rowNum % ROWS_PER_PAGE;
    int bytesOffset = rowOffset * ROW_SIZE;
    return page + bytesOffset;
}

InputBuffer *newInputBuffer()
{
    InputBuffer *inputBuffer = (InputBuffer *)malloc(sizeof(InputBuffer));
    inputBuffer->buffer = NULL;
    inputBuffer->buffer_length = 0;
    inputBuffer->input_length = 0;
    return inputBuffer;
}

void printPrompt()
{
    printf("cdb > ");
}

ssize_t getline(char **buffer, size_t *buffer_length, FILE *stream)
{
    if (buffer == NULL || buffer_length == NULL || stream == NULL)
    {
        return -1;
    }

    size_t pos = 0;
    int c;

    if (*buffer == NULL)
    {
        *buffer_length = 128;
        *buffer = malloc(*buffer_length);
        if (*buffer == NULL)
        {
            return -1;
        }
    }

    while ((c = fgetc(stream)) != EOF && c != '\n')
    {
        if (pos + 1 >= *buffer_length)
        {
            *buffer_length *= 2;
            char *new_buffer = realloc(*buffer, *buffer_length);
            if (new_buffer == NULL)
            {
                return -1;
            }
            *buffer = new_buffer;
        }
        (*buffer)[pos++] = (char)c;
    }

    if (pos == 0 && c == EOF)
    {
        return -1;
    }

    (*buffer)[pos] = '\0';
    return pos;
}

void readInput(InputBuffer *inputBuffer)
{
    ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->buffer_length), stdin);
    if (bytesRead <= 0)
    {
        return;
        exit(EXIT_FAILURE);
    }
    inputBuffer->buffer_length = bytesRead;
    inputBuffer->buffer[bytesRead] = 0;
}

MetaCommandResult doMetaCommand(InputBuffer *inputBuffer)
{
    if (strcmp(inputBuffer->buffer, ".exit") == 0)
    {
        return META_COMMAND_SUCCESS;
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepareInsert(InputBuffer *inputBuffer, Statement *statement)
{
    statement->type = STATEMENT_INSERT;
    char *keyword = strtok(inputBuffer->buffer, " ");
    char *idString = strtok(NULL, " ");
    char *username = strtok(NULL, " ");
    char *email = strtok(NULL, " ");

    if (idString == NULL || username == NULL || email == NULL)
    {
        return PREPARE_SYNTAX_ERROR;
    }
    int id = atoi(idString);
    if (id < 0)
    {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE)
    {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->rowToInsert.id = id;
    strcpy(statement->rowToInsert.username, username);
    strcpy(statement->rowToInsert.email, email);
    return PREPARE_SUCCESS;
}

PrepareResult prepareStatement(InputBuffer *inputBuffer, Statement *statement)
{
    if (strncmp(inputBuffer->buffer, "insert", 6) == 0)
    {
        return prepareInsert(inputBuffer, statement);
    }
    if (strncmp(inputBuffer->buffer, "select", 6) == 0)
    {
        statement->type = STATEMENT_SELECT;

        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult executeInsert(Statement *statement, Table *table)
{
    if (table->numRows >= TABLE_MAX_ROWS)
    {
        return EXECUTE_TABLE_FULL;
    }
    Row *rowToInsert = &(statement->rowToInsert);
    serializeRow(rowToInsert, rowSlot(table, table->numRows));
    table->numRows += 1;
    printf("Inserted 1\n");
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement *statement, Table *table)
{
    Row row;
    for (int i = 0; i < table->numRows; i++)
    {
        deserializeRow(rowSlot(table, i), &row);
        printf("%d, %s, %s\n", row.id, row.email, row.username);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement *statement, Table *table)
{
    switch (statement->type)
    {
    case STATEMENT_INSERT:
        return executeInsert(statement, table);
    case STATEMENT_SELECT:
        return executeSelect(statement, table);
    }
}

Table *newTable()
{
    Table *table = (Table *)malloc(sizeof(Table));
    table->numRows = 0;
    for (int i = 0; i < TABLE_MAX_PAGES; i++)
    {
        table->pages[i] = NULL;
    }
    return table;
}

void freeTable(Table *table)
{
    for (int i = 0; table->pages[i]; i++)
    {
        free(table->pages[i]);
    }
    free(table);
}

void closeBuffer(InputBuffer *inputBuffer)
{
    free(inputBuffer->buffer);
    free(inputBuffer);
}

int main(int argc, char const *argv[])
{
    Table *table = newTable();
    InputBuffer *inputBuffer = newInputBuffer();
    while (true)
    {
        printPrompt();
        readInput(inputBuffer);
        if (inputBuffer->buffer[0] == '.')
        {
            switch (doMetaCommand(inputBuffer))
            {
            case META_COMMAND_SUCCESS:
                freeTable(table);
                closeBuffer(inputBuffer);
                continue;
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Error: Unrecognized command '%s'.\n", inputBuffer->buffer);
                continue;
            }
        }
        Statement statement;
        switch (prepareStatement(inputBuffer, &statement))
        {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_NEGATIVE_ID:
            printf("Id must be positive.\n");
            continue;
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement.\n");
            continue;
        case (PREPARE_STRING_TOO_LONG):
            printf("Fields too long.\n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Error: Unrecognized keyword at start of '%s'.\n", inputBuffer->buffer);
            continue;
        }
        
        switch (executeStatement(&statement, table))
        {
        case EXECUTE_TABLE_FULL:
            printf("Table full");
            break;
        default:
            break;
        }
    }
    return 0;
}
