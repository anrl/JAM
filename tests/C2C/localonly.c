
jasync localme(int c, char *s)
{
  while(1)
    {
      jsleep(200);
      printf("############-->>> Hello  ME  %d, %s\n", c, s);
    }
}

jasync localyou(int c, char *s)
{
  while(1)
    {
      jsleep(400);
      printf("############-->>> Hello YOU  %d, %s\n", c, s);
    }
}

int main(int argc, char *argv[])
{
  localme(10, "cxxxxxxxx");
  localyou(10, "cxxxxxxxx");

}
