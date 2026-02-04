const { createApp, ref, onMounted } = Vue;
const API_URL = 'http://localhost:8000/api';

createApp({
    setup() {
        const operadoras = ref([]);
        const stats = ref({});
        const page = ref(1);
        const totalPages = ref(1);
        const loading = ref(false);
        const searchQuery = ref('');
        const currentView = ref('lista');
        const selectedOperadora = ref(null);
        const despesasHistorico = ref([]);
        let chartInstance = null;

        const fetchOperadoras = async (p = 1) => {
            loading.value = true;
            try {
                const res = await fetch(`${API_URL}/operadoras?page=${p}&limit=10&search=${searchQuery.value}`);
                if(!res.ok) throw new Error("Erro na API");
                const data = await res.json();
                operadoras.value = data.data;
                page.value = data.page;
                totalPages.value = Math.ceil(data.total / data.limit);
            } catch (e) {
                console.error(e);
                alert('Erro de conexão com o Backend.');
            } finally {
                loading.value = false;
            }
        };

        const verDetalhes = async (cnpj) => {
            if (!cnpj) return;
            loading.value = true;
            try {
                const resOp = await fetch(`${API_URL}/operadoras/${cnpj}`);
                selectedOperadora.value = await resOp.json();
                
                const resDesp = await fetch(`${API_URL}/operadoras/${cnpj}/despesas`);
                despesasHistorico.value = await resDesp.json();
            } catch (e) {
                alert('Erro ao carregar detalhes.');
            } finally {
                loading.value = false;
            }
        };

        const loadDashboard = async () => {
            currentView.value = 'dashboard';
            loading.value = true;
            try {
                const res = await fetch(`${API_URL}/estatisticas`);
                stats.value = await res.json();
                setTimeout(() => renderChart(stats.value.top_5_operadoras), 100);
            } catch (e) {
                console.error(e);
            } finally {
                loading.value = false;
            }
        };

        const renderChart = (top5) => {
            const ctx = document.getElementById('chartTop5');
            if (!ctx) return;
            if (chartInstance) chartInstance.destroy();
            
            chartInstance = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: top5.map(d => d.razao_social.substring(0, 15) + '...'),
                    datasets: [{
                        label: 'Total de Despesas (R$)',
                        data: top5.map(d => d.total_despesas),
                        backgroundColor: '#0d6efd',
                        borderRadius: 4
                    }]
                },
                options: { responsive: true, plugins: { legend: { display: false } } }
            });
        };

        const formatMoney = (val) => {
            if (val === null || val === undefined) return 'R$ 0,00';
            return new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);
        };

        onMounted(() => fetchOperadoras());

        return {
            operadoras, stats, page, totalPages, loading, searchQuery, currentView,
            selectedOperadora, despesasHistorico,
            fetchOperadoras, verDetalhes, loadDashboard, formatMoney
        };
    }
}).mount('#app');