#include <unistd.h>

void f_param(int d);
void f_no_param();

int main(int argc, char* argv[])
{
    sleep(2);
    f_param(4);
    f_no_param();
    f_param(8);
    f_no_param();
    return 0;
}
