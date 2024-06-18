query_0 = """
            SELECT datapreenc, COUNT(cont) AS cont
            FROM (
                SELECT FROM_TIMESTAMP(data_hora_inclusao, 'dd/MM/yy') AS datapreenc,
                    numero_ocorrencia AS cont
                FROM db_bisp_reds_reporting.tb_ocorrencia
                WHERE data_hora_inclusao BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND NOW()
                AND digitador_id_orgao = 0
            ) AS subquery
            GROUP BY datapreenc
            ORDER BY CAST(TO_TIMESTAMP(datapreenc, 'dd/MM/yy') AS DATE) DESC;
        """
query_1 = """
            -- 1 - REDS_RAT - OK
            SELECT	
                OCO.numero_ocorrencia AS "RAT.NUM_ATIVIDADE",
                OCO.natureza_codigo AS "NAT.CODIGO",
                OCO.natureza_descricao AS "NAT.DESCRICAO",
                FROM_TIMESTAMP(OCO.data_hora_inclusao, 'dd/MM/yyyy') AS DTA_HRA_INCLUSAO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'dd/MM/yyyy') AS DTA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'HH:mm') AS HRA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'dd/MM/yyyy') AS DTA_TERMINO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'HH:mm') AS HRA_TERMINO,
                OCO.complemento_natureza_descricao AS 'DES_ALVO_EVENTO',
                OCO.local_imediato_descricao AS 'DES_LUGAR',
                OCO.nome_operacao AS 'NOM_OPERACAO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNID_RESPONSAVEL',
                OCO.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                OCO.logradouro_nome AS 'LOGRADOURO',
                OCO.descricao_endereco AS 'DES_ENDERECO',
                OCO.numero_endereco AS 'NUM_ENDERECO',
                OCO.complemento_alfa AS 'COMPLEMENTO_ALFA',
                OCO.descricao_complemento_endereco AS 'COMPLEMENTO_ENDERECO',
                OCO.numero_complementar AS 'NUM_COMPLEMENTAR',
                OCO.codigo_bairro AS 'COD_BAIRRO',
                OCO.nome_bairro AS 'NOME_BAIRRO',
                OCO.tipo_logradouro2_descricao AS 'TIPO_LOGRADOURO2',
                OCO.logradouro2_nome AS 'LOGRADOURO2',
                OCO.descricao_endereco_2 AS 'DES_ENDERECO2',
                NULLIF(CAST(OCO.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                OCO.nome_municipio AS 'MUNICIPIO',
                OCO.numero_latitude AS 'LATITUDE',
                OCO.numero_longitude AS 'LONGITUDE',
                MASTER.codigo_unidade_area AS 'COD_UNIDADE_AREA',
                MASTER.unidade_area_militar_nome AS 'NOM_UNIDADE_AREA',
                CONCAT('PM',OCO.digitador_matricula) AS 'DIGITADOR'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg AS 'MASTER'
                ON OCO.id_local = MASTER.id_local
            WHERE 1=1
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """
query_2 = """
            -- 2 -REDS_RAT_EFETIVOS - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                OCO.digitador_matricula AS 'NUM_MATRICULA',
                OCO.digitador_nome AS 'NOME',
                OCO.digitador_cargo_efetivo AS 'NOM_CARGO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNIDADE'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            WHERE 1=1
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """
query_3 = """
        -- 3 - REDS_RAT_PRODUTIVIDADE
        SELECT	      
            PROD.numero_ocorrencia AS 'RAT.NUM_ATIVIDADE',
            PROD.indicador_descricao AS 'DESCRICAO',
            PROD.quantidade AS 'QUANTIDADE'	
        FROM
            db_bisp_reds_reporting.tb_ocorrencia OCO
        LEFT JOIN
            db_bisp_reds_reporting.tb_rat_produtividade_ocorrencia PROD
            ON OCO.numero_ocorrencia = PROD.numero_ocorrencia
        WHERE 1=1
        AND OCO.data_hora_fato IS NOT NULL
        AND PROD.data_hora_fato BETWEEN '{}' AND '{}'
        AND PROD.quantidade <> 0
        ORDER BY PROD.data_hora_fato;
        """
query_4 = """
            -- 4 - REDS_RAT_VIATURAS - OK
            SELECT	
                VTR.numero_ocorrencia AS 'NUM_ATIVIDADE',
                VTR.numero_sequencial_viatura AS 'NUM_SEQ_RECURSO',
                VTR.placa AS 'NUM_PLACA',
                VTR.numero_reg AS 'NUM_PREFIXO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia AS OCO
            LEFT JOIN db_bisp_reds_reporting.tb_viatura_ocorrencia VTR
                ON  OCO.numero_ocorrencia = VTR.numero_ocorrencia
            WHERE 1=1
            AND orgao_sigla = 'PM'
            AND OCO.nome_tipo_relatorio = 'RAT'
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND VTR.data_hora_fato BETWEEN '{}' AND '{}'
            ORDER BY VTR.data_hora_fato;
        """
query_5 = """
            -- 5 - REDS_BOS - OK
            SELECT	
                OCO.numero_ocorrencia AS "RAT.NUM_ATIVIDADE",
                OCO.natureza_codigo AS "NAT.CODIGO",
                OCO.natureza_descricao AS "NAT.DESCRICAO",
                FROM_TIMESTAMP(OCO.data_hora_inclusao, 'dd/MM/yyyy') AS 'DTA_HRA_INCLUSAO',
                FROM_TIMESTAMP(OCO.data_hora_fato, 'dd/MM/yyyy') AS DTA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_fato, 'HH:mm') AS HRA_INICIO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'dd/MM/yyyy') AS DTA_TERMINO,
                FROM_TIMESTAMP(OCO.data_hora_final, 'HH:mm') AS HRA_TERMINO,
                OCO.complemento_natureza_descricao AS 'DES_ALVO_EVENTO',
                OCO.local_imediato_descricao AS 'DES_LUGAR',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNID_RESPONSAVEL',
                OCO.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                OCO.logradouro_nome AS 'LOGRADOURO',
                OCO.descricao_endereco AS 'DES_ENDERECO',
                OCO.numero_endereco AS 'NUM_ENDERECO',
                OCO.complemento_alfa AS 'COMPLEMENTO_ALFA',
                OCO.descricao_complemento_endereco AS 'COMPLEMENTO_ENDERECO',
                OCO.numero_complementar AS 'NUM_COMPLEMENTAR',
                OCO.codigo_bairro AS 'COD_BAIRRO',
                OCO.nome_bairro AS 'NOME_BAIRRO',
                OCO.tipo_logradouro2_descricao AS 'TIPO_LOGRADOURO2',
                OCO.logradouro2_nome AS 'LOGRADOURO2',
                OCO.descricao_endereco_2 AS 'DES_ENDERECO2',
                NULLIF(CAST(OCO.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                OCO.nome_municipio AS 'MUNICIPIO',
                OCO.numero_latitude AS 'LATITUDE',
                OCO.numero_longitude AS 'LONGITUDE',
                MASTER.codigo_unidade_area AS 'COD_UNIDADE_AREA',
                MASTER.unidade_area_militar_nome AS 'NOM_UNIDADE_AREA',
                CONCAT('PM',OCO.digitador_matricula) AS 'DIGITADOR'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_master.tb_local_unidade_area_pmmg AS MASTER
                ON OCO.id_local = MASTER.id_local
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """
query_6 = """
            -- 6 - REDS_BOS_EFETIVOS - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                OCO.digitador_matricula AS 'NUM_MATRICULA',
                OCO.digitador_nome AS 'NOME',
                OCO.digitador_cargo_efetivo AS 'NOM_CARGO',
                OCO.unidade_responsavel_registro_codigo AS 'COD_UNIDADE_SERVICO',
                OCO.unidade_responsavel_registro_nome AS 'NOM_UNIDADE'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            ORDER BY OCO.data_hora_fato;
        """
query_7 = """
            -- 7 - REDS_BOS_ENVOLVIDO - OK
            SELECT	
                OCO.numero_ocorrencia AS 'NUM_ATIVIDADE',
                ENV.nome_completo_envolvido AS 'NOM_ENVOLVIDO',
                ENV.nome_mae AS 'NOM_MAE',
                ENV.envolvimento_descricao AS 'TIPO_ENVOLVIMENTO',
                ENV.tipo_logradouro_descricao AS 'TIPO_LOGRADOURO',
                ENV.logradouro_nome AS 'LOGRADOURO',
                ENV.numero_endereco AS 'NUM_ENDERECO',
                ENV.compl_alfa AS 'COMPLEMENTO_ALFA',
                ENV.descricao_complementar_endereco AS 'COMPLEMENTO_ENDERECO',
                ENV.numero_complementar AS 'NUM_COMPLEMENTAR',
                ENV.codigo_bairro AS 'COD_BAIRRO',
                ENV.nome_bairro AS 'NOME_BAIRRO',
                NULLIF(CAST(ENV.codigo_municipio AS INT), 0) AS "COD_MUNICIPIO",
                MUN.dsmunicipiosemacentomaiusc AS 'MUNICIPIO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia OCO
            LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS ENV
                ON OCO.numero_ocorrencia = ENV.numero_ocorrencia
            LEFT JOIN db_bisp_shared.tb_dim_municipio AS MUN
                ON ENV.codigo_municipio = MUN.cdmunicipioibge6
            WHERE 1=1
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND OCO.data_hora_alteracao BETWEEN '{}' AND '{}'
            AND ENV.nome_completo_envolvido <> ''
            ORDER BY OCO.data_hora_fato;
        """
query_8 = """
            -- 8 - REDS_BOS_VIATURAS - OK
            SELECT	
                VTR.numero_ocorrencia AS 'NUM_ATIVIDADE',
                VTR.numero_sequencial_viatura AS 'NUM_SEQ_RECURSO',
                VTR.placa AS 'NUM_PLACA',
                VTR.numero_reg AS 'NUM_PREFIXO'
            FROM
                db_bisp_reds_reporting.tb_ocorrencia AS OCO
            LEFT JOIN db_bisp_reds_reporting.tb_viatura_ocorrencia VTR
                ON  OCO.numero_ocorrencia = VTR.numero_ocorrencia
            WHERE 1=1
            AND orgao_sigla = 'PM'
            AND OCO.nome_tipo_relatorio IN ('BOS', 'BOS AMPLO')
            AND OCO.ind_estado IN ('R', 'F')
            AND OCO.data_hora_fato IS NOT NULL
            AND VTR.data_hora_fato BETWEEN '{}' AND '{}'
            ORDER BY VTR.data_hora_fato;
        """