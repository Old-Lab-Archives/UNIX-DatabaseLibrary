#include "SugarDB.h"

/* Allocating and Initializing a database structure and all the buffers it requires. */
DB *_db_alloc(int namelen)
{
	DB *db;
	/* use calloc initializing the structure to 0 */
	if((db = calloc(1, sizeof(DB))) == NULL)
		err_dump("error");
	db->idxfd = db->datfd = -1; /* descriptors */
	/* Allocation begins 
	+5 for ".idx" or ".dat" and also NULL at end.
	*/
	if((db->name = malloc(namelen + 5)) == NULL)
		err_dump("error");
	/* Allocating an index buffer and a data buffer
	+2 for newline and NULL at end.
	*/
	if((db->idxbuf = malloc(IDXLEN_MAX + 2)) == NULL)
		err_dump("error");
	if((db->datbuf = malloc(DATLEN_MAX + 2)) == NULL)
		err_dump("error");
	return(db);
}
