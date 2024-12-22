#include <stdio.h>
#include <string.h>
#include <stdbool.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
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
    __ssize_t input_length;
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

typedef struct{
    int fileDescriptor;
    int fileLength;
    void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
    int numRows;
    Pager* pager;
} Table;

void serializeRow(Row *source, void *destination)
{
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
    strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserializeRow(void *source, Row *destination)
{
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* getPage(Pager* pager, int pageNum){
    if(pageNum > TABLE_MAX_PAGES){
        printf("Tried to fetch page index out of bounds for: %d\n", pageNum);
        exit(EXIT_FAILURE);
    }
    if(pager->pages[pageNum] != NULL){
        return pager->pages[pageNum];
    }
    void* page =  malloc(PAGE_SIZE);
    int numPages = pager->fileLength / PAGE_SIZE;
    if(pager->fileLength % PAGE_SIZE){
        numPages+=1;
    }
    if(pageNum <= numPages){
        lseek(pager->fileDescriptor, pageNum* PAGE_SIZE, SEEK_SET);
        ssize_t bytesRead = read(pager->fileDescriptor, page, PAGE_SIZE);
        if(bytesRead == -1){
            printf("Error reading file: %d\n", errno);
        }
    }
    pager->pages[pageNum] = page;
    return page;
}

void *rowSlot(Table *table, int rowNum)
{
    int pageNum = rowNum / ROWS_PER_PAGE;
    void *page = getPage(table->pager, pageNum);
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
    __ssize_t bytesRead = getline(&(inputBuffer->buffer), &(inputBuffer->buffer_length), stdin);
    if (bytesRead <= 0)
    {
        exit(EXIT_FAILURE);
    }
    inputBuffer->buffer_length = bytesRead -1;
    inputBuffer->buffer[bytesRead - 1] = 0;
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

Pager* pagerOpen(const char* fileName){
    int fd = open(fileName, O_RDWR | O_CREAT);
    if(fd == -1){
        printf("Unable to open file named: %s\n", fileName);
        exit(EXIT_FAILURE);
    }
    off_t fileLength = lseek(fd,0,SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    if (pager == NULL) {
        printf("Memory allocation failed for pager.\n");
        exit(EXIT_FAILURE);
    }
    pager->fileDescriptor = fd;
    pager->fileLength = fileLength;

    for (size_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        pager->pages[i] = NULL;
    }
    return pager;
    
}

Table *dbOpen(const char* fileName)
{
    Pager* pager = pagerOpen(fileName);
    Table *table = (Table *)malloc(sizeof(Table));
    table->numRows = pager->fileLength/ ROW_SIZE;
    table->pager = pager;
    return table;
}

void pagerFlush(Pager* pager, int pageNum, int size){
    if(pager->pages[pageNum] == NULL){
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    off_t offset = lseek(pager->fileDescriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if(offset == -1){
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytesWritten = write(pager->fileDescriptor, pager->pages[pageNum], size);
    if(bytesWritten == -1){
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void dbClose(Table *table)
{
    Pager* pager = table->pager;
    int numOfFullPages = table->numRows / ROWS_PER_PAGE;
    for (size_t i = 0; i < numOfFullPages; i++)
    {
        if(pager->pages[i] == NULL){
            continue;
        }
        pagerFlush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
    int numOfAdditionalRows = table->numRows % ROWS_PER_PAGE;
    if(numOfAdditionalRows > 0){
        int pageNum = numOfFullPages;
        if(pager->pages[pageNum] != NULL){
            pagerFlush(pager, pageNum, numOfAdditionalRows * ROW_SIZE);
            free(pager->pages[pageNum]);
            pager->pages[pageNum] = NULL;
        }
    }
    int result = close(pager->fileDescriptor);
    if(result == -1){
        printf("Error closing db file: %d\n", errno);
        exit(EXIT_FAILURE);
    }
    for (size_t i = 0; i < TABLE_MAX_PAGES; i++)
    {
        if(pager->pages[i]){
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

void closeBuffer(InputBuffer *inputBuffer)
{
    free(inputBuffer->buffer);
    free(inputBuffer);
}

MetaCommandResult doMetaCommand(InputBuffer *inputBuffer, Table* table)
{
    if (strcmp(inputBuffer->buffer, ".exit") == 0)
    {
        dbClose(table);
        closeBuffer(inputBuffer);
        printf("Bye!\n");
        return META_COMMAND_SUCCESS;
    }
    else
    {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

int main(int argc, char *argv[])
{
    if(argc < 2){
        printf("Must supply a database file name!\n");
        exit(EXIT_FAILURE);
    }
    char* fileName = argv[1];
    Table *table = dbOpen(fileName);
    InputBuffer *inputBuffer = newInputBuffer();
    while (true)
    {
        printPrompt();
        readInput(inputBuffer);
        if (inputBuffer->buffer[0] == '.')
        {
            switch (doMetaCommand(inputBuffer, table))
            {
            case META_COMMAND_SUCCESS:
                exit(EXIT_SUCCESS);
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
