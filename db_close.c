#include "SugarDB.h"

void db_close(DB *db)
{
	_db_free(db); /* closes file descriptors, free the buffers & struct */
}
