
.PHONY: all
all: test1 test2 test3

test1: test_assign3_1.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c 
	gcc -o test1 test_assign3_1.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c

test2: test_expr.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c
	gcc -o test2 test_expr.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c

test3: test_assign3_2.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c 
	gcc -o test3 test_assign3_2.c record_mgr.c rm_serializer.c expr.c buffer_mgr_stat.c storage_mgr.c dberror.c buffer_mgr.c

.PHONY: clean
clean:
	rm test1 test2 test3
