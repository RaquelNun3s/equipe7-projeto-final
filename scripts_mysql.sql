#---------------------------------------------------------------------------------------------------------------------------------------

CREATE DATABASE original;

CREATE DATABASE original2;

DROP DATABASE original2;

#---------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE auditoria (
     id MEDIUMINT NOT NULL AUTO_INCREMENT,
     data date NOT NULL,
     num_id_inserido INT NOT NULL,
     usuario VARCHAR (32) NOT NULL,
     PRIMARY KEY (id)
);

#---------------------------------------------------------------------------------------------------------------------------------------

ALTER TABLE auditoria ADD teste varchar(255);

ALTER TABLE auditoria DROP COLUMN teste;

#---------------------------------------------------------------------------------------------------------------------------------------

DELIMITER //

CREATE TRIGGER auditoria_after_insert
AFTER INSERT
   ON autuacao FOR EACH ROW

BEGIN

   DECLARE vUser varchar(50);

   -- Seleciona o usuário que realizou a inserção
   SELECT USER() INTO vUser;

   -- Definindo a inserção na tabela auditoria
   INSERT INTO auditoria
   ( id,
     data,
     num_id_inserido,
     usuario)
   VALUES
   ( NULL,
     SYSDATE(),
     NEW.id_,
     vUser );

END; //

DELIMITER ;

#---------------------------------------------------------------------------------------------------------------------------------------

INSERT INTO autuacao (id_) VALUES (01);

DELETE FROM autuacao WHERE id_ = 01;

UPDATE autuacao SET id_ = 02 WHERE id_ = 01;

SELECT * FROM auditoria;

SELECT * FROM autuacao;
