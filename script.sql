-- Índices
analyse caso;
explain select * from caso where data_inicio_sintomas >= '2021-01-01';
create index IdxCasoInicioSintomas on caso(data_inicio_sintomas);

analyse paciente;
explain select * from paciente where genero = 'FEMININO';
create extension btree_gin;
create index IdxPacienteGenero on paciente using gin (genero);

analyse paciente;
explain select genero from paciente where idade >= 60;
create index IdxPacienteIdade on paciente(genero) where idade >= 60;

analyse comorbidade_paciente;
analyse comorbidade;
explain select * 
from comorbidade_paciente
join comorbidade on comorbidade.codigo = comorbidade_paciente.comorbidade  
where comorbidade.descricao = 'Diabetes';
create index IdxComorbidadePacienteComorbidade on comorbidade_paciente(comorbidade);

analyse paciente;
explain select genero from paciente where idade <= 12;
create index IdxPacienteIdadeCrianca on paciente(genero) where idade <= 12;


--A administração estadual possui acesso total para alterações de dados nas tabelas de regiao administrativa, municipio e isolamento
create user administracao_estadual;
grant select, insert, update, delete on regiao_administrativa to administracao_estadual;
grant select, insert, update, delete on municipio to administracao_estadual;
grant select, insert, update, delete on isolamento to administracao_estadual;


--O médico possui privilégio total para alterar os dados das tabelas de caso, comorbidade do paciente, e pacientes
--Possui privilégio de visualização nas tabelas de comorbidade e municipio 
create user medico;
grant select, insert, update, delete on caso to medico;
grant select, insert, update, delete on comorbidade_paciente to medico;
grant select, insert, update, delete on paciente to medico;
grant select on comorbidade to medico;
grant select on municipio to medico;
grant select on regiao_administrativa to medico;


--O recepcionista possui privilégio total na tabela de paciente
--Possui privilégio de visualização nas tabelas de município e região administrativa  
create user recepcionista;
grant select, insert, update, delete on paciente to recepcionista;
grant select on municipio to recepcionista;
grant select on regiao_administrativa to recepcionista;


--Cria campo data de óbito
alter table paciente add column data_obito date;

--Cria função que atualiza a data de óbito do paciente para a data atual da atualização do caso para óbito
create or replace function preencheDataObitoPaciente()
returns trigger as $$
begin
	if not old.obito and new.obito then 
		update paciente set data_obito = current_date where codigo = new.paciente;
	end if;
	return new;
end;
$$ language plpgsql;
create trigger TriggerPreencheDataObitoPaciente 
before insert or update on caso
for each row execute procedure preencheDataObitoPaciente();

--Transações
begin;
set transaction isolation level read committed;

select codigo from comorbidade where descricao = 'Comorbidade exemplo';

insert into paciente (codigo, genero, idade, municipio) values 
((select max(paciente.codigo) + 1 from paciente), 
'MASCULINO',
25,
3550308);

insert into comorbidade_paciente (paciente, comorbidade) values 
((select max(paciente.codigo) from paciente), (select codigo from comorbidade where descricao = 'Comorbidade exemplo'));

commit;

--Inclui um novo registro na tabela de comorbidades que, posteriormente ao commit dessa transação, a transação 1 utilizará em um insert	
begin;
set transaction isolation level read committed;

insert into comorbidade (codigo, descricao) values ((select max(codigo) + 1 from comorbidade), 'Comorbidade exemplo');
 
commit;
 




