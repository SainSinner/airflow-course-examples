from jinja2 import Template

# Напишите функцию, только с Jinja выражением, которая выводит четную последовательность от 1 до N-1. Пробел между цифрами одиночный!
# x = 8
x = int(input())
t = Template("{% for n in range(2,x,2) %}{{n}} " "{% endfor %}")
print(t.render(x=x))
