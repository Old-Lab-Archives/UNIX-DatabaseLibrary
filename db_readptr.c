#include "SugarDB"
/* Read a chain ptr field from anywhere in the index file:
--- the free list pointer, hash table chain ptr, or index record chain ptr */
off_t _db_readptr(DB *db, off_t offset)
{
	char asciiptr[PTR_SZ + 1];
	if(lseek(db->idxfd, offset, SEEK_SET) == -1)
		err_dump("error");
	if(read(db->idxfd, asciiptr, PTR_SZ) != PTR_SZ)
		err_dump("error");
	asciiptr[PTR_SZ] = 0; /* null terminate */
	return(atol(asciiptr));
}
