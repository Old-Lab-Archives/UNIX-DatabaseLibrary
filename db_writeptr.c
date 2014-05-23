#include "SugarDB.h"

/* Writing a chain ptr field somewhere in the index file:
	=== The free list
	=== The hash table
	=== index record
*/
void _db_writeptr(DB *db, off_t offset, off_t ptrval)
{
	char asciiptr[PTR_SZ + 1];
	if(ptrval < 0 || ptrval > PTR_MAX)
		err_quit("invalid ptr: %d", ptrval);
	sprintf(asciiptr, "%*d", PTR_SZ, ptrval);
	if(lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("error");
	if(write(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("error");
}
