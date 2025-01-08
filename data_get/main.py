from utils import download


if __name__ == '__main__':
    base_url_IBGE = "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2022/Agregados_por_Setores_Censitarios/Agregados_por_Municipio_csv/"

    zips_to_download = {"municipios": "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2022/UFs/BA/BA_Municipios_2022.zip",
                        "partido": "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_partido_munzona/votacao_partido_munzona_2022.zip",
                        "alfabetizacao": "{}/Agregados_por_municipios_alfabetizacao_BR.zip".format(base_url_IBGE),
                        "basico": "{}/Agregados_por_municipios_basico_BR.zip".format(base_url_IBGE),
                        "domicilio1": "{}/Agregados_por_municipios_caracteristicas_domicilio1_BR.zip".format(base_url_IBGE),
                        "domicilio2": "{}/Agregados_por_municipios_caracteristicas_domicilio2_BR.zip".format(base_url_IBGE),
                        "domicilio3": "{}/Agregados_por_municipios_caracteristicas_domicilio3_BR.zip".format(base_url_IBGE),
                        "cor_raca": "{}/Agregados_por_municipios_cor_ou_raca_BR.zip".format(base_url_IBGE),
                        "demografia": "{}/Agregados_por_municipios_demografia_BR.zip".format(base_url_IBGE),
                        "domicilio_indigena": "{}/Agregados_por_municipios_domicilios_indigenas_BR.zip".format(base_url_IBGE),
                        "domicilio_quilombolas": "{}/Agregados_por_municipios_domicilios_quilombolas_BR.zip".format(base_url_IBGE),
                        "obitos": "{}/Agregados_por_municipios_obitos_BR.zip".format(base_url_IBGE),
                        "parentesco": "{}/Agregados_por_municipios_parentesco_BR.zip".format(base_url_IBGE),
                        "indigenas": "{}/Agregados_por_municipios_pessoas_indigenas_BR.zip".format(base_url_IBGE),
                        "quilombolas": "{}/Agregados_por_municipios_pessoas_quilombolas_BR.zip".format(base_url_IBGE),
                        "candidato": "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona/votacao_candidato_munzona_2022.zip",
                        'censo_agro': "https://files.catbox.moe/aojnd0.zip",
                        "bolsa_familia": 'https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:2021*&fq=tipo_s:mes_mu&wt=csv&q=*&fl=ibge:codigo_ibge,anomes:anomes_s,qtd_familias_beneficiarias_bolsa_familia,valor_repassado_bolsa_familia&rows=10000000&sort=anomes_s%20asc,%20codigo_ibge%20asc',
                        'cad_unico': 'https://aplicacoes.mds.gov.br/sagi/servicos/misocial?fq=anomes_s:2022*&wt=csv&omitHeader=true&fq=cadunico_tot_fam_i:{0%20TO%20*]&q=*&fl=ibge:codigo_ibge,anomes:anomes_s,cadunico_tot_fam:cadunico_tot_fam_i,cadunico_tot_pes:cadunico_tot_pes_i,cadunico_tot_fam_rpc_ate_meio_sm:cadunico_tot_fam_rpc_ate_meio_sm_i,cadunico_tot_pes_rpc_ate_meio_sm:cadunico_tot_pes_rpc_ate_meio_sm_i,cadunico_tot_fam_pob:cadunico_tot_fam_pob_i,cadunico_tot_pes_pob:cadunico_tot_pes_pob_i,cadunico_tot_fam_ext_pob:cadunico_tot_fam_ext_pob_i,cadunico_tot_pes_ext_pob:cadunico_tot_pes_ext_pob_i,cadunico_tot_fam_pob_e_ext_pob:cadunico_tot_fam_pob_e_ext_pob_i,cadunico_tot_pes_pob_e_ext_pob:cadunico_tot_pes_pob_e_ext_pob_i&rows=100000000&sort=anomes_s%20desc,%20codigo_ibge%20asc'}

    for name, download_url in zips_to_download.items():
        download(name=name,download_url=download_url)

    csv_to_save = {}