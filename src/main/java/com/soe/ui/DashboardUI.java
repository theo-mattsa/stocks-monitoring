package com.soe.ui;

import com.formdev.flatlaf.FlatDarkLaf;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.text.NumberFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class DashboardUI {
    private static DashboardUI instance;
    private JFrame frame;
    private DefaultTableModel tableModel;
    private JTextArea eventLog;
    private JLabel marketStatus, symbolCount, eventCount;
    private JTabbedPane tabbedPane;
    private int totalEvents = 0, unreadEvents = 0;

    private final NumberFormat priceFormat = NumberFormat.getNumberInstance(Locale.US);
    private final NumberFormat percentFormat = NumberFormat.getPercentInstance(Locale.US);
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private DashboardUI() {
        priceFormat.setMinimumFractionDigits(4); priceFormat.setMaximumFractionDigits(4);
        percentFormat.setMinimumFractionDigits(0); percentFormat.setMaximumFractionDigits(4);
    }

    public static DashboardUI getInstance() {
        if (instance == null) instance = new DashboardUI();
        return instance;
    }

    public static boolean isGuiInitialized() {
        return instance != null && instance.frame != null && instance.frame.isVisible();
    }

    public void start() {
        SwingUtilities.invokeLater(() -> {
            FlatDarkLaf.setup();
            
            // Global UI font scaling
            UIManager.put("defaultFont", new Font("SansSerif", Font.PLAIN, 15));

            frame = new JFrame("Sistema de Monitoramento de Ações");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(1000, 650);
            frame.setLocationRelativeTo(null);

            JPanel mainPanel = new JPanel(new BorderLayout(0, 15));
            mainPanel.setBorder(BorderFactory.createEmptyBorder(18, 18, 18, 18));

            // Header bar
            JPanel topPanel = new JPanel(new BorderLayout());
            JLabel title = new JLabel("PAINEL DE MERCADO");
            title.setFont(new Font("SansSerif", Font.BOLD, 22));
            title.setForeground(new Color(0, 229, 255));

            JPanel statusGroup = new JPanel(new FlowLayout(FlowLayout.RIGHT, 18, 0));
            marketStatus = new JLabel("● MERCADO: AO VIVO");
            marketStatus.setForeground(new Color(46, 204, 113));
            marketStatus.setFont(new Font("SansSerif", Font.BOLD, 15));

            symbolCount = new JLabel("ATIVOS: 0");
            eventCount = new JLabel("EVENTOS: 0");

            statusGroup.add(marketStatus);
            statusGroup.add(symbolCount);
            statusGroup.add(eventCount);
            topPanel.add(title, BorderLayout.WEST);
            topPanel.add(statusGroup, BorderLayout.EAST);

            // Tabbed container
            tabbedPane = new JTabbedPane();

            // Tab 1: Live Quotes
            String[] cols = {"ATIVO", "PREÇO", "VARIAÇÃO"};
            tableModel = new DefaultTableModel(cols, 0) {
                @Override
                public boolean isCellEditable(int r, int c) { return false; }
            };
            JTable quoteTable = new JTable(tableModel);
            quoteTable.setRowHeight(38);
            quoteTable.setFont(new Font("SansSerif", Font.PLAIN, 16));
            quoteTable.getTableHeader().setFont(new Font("SansSerif", Font.BOLD, 16));

            // Gain/loss cell rendering
            quoteTable.getColumnModel().getColumn(2).setCellRenderer(new DefaultTableCellRenderer() {
                @Override
                public Component getTableCellRendererComponent(JTable t, Object v, boolean s, boolean f, int r, int c) {
                    Component comp = super.getTableCellRendererComponent(t, v, s, f, r, c);
                    if (v != null) {
                        String val = v.toString();
                        comp.setForeground(val.startsWith("+") ? new Color(46, 204, 113) : 
                                         val.startsWith("-") ? new Color(231, 76, 60) : t.getForeground());
                    }
                    return comp;
                }
            });

            tabbedPane.addTab("  Cotações em Tempo Real  ", new JScrollPane(quoteTable));

            // Tab 2: Market Events Log
            eventLog = new JTextArea();
            eventLog.setEditable(false);
            eventLog.setFont(new Font("Monospaced", Font.PLAIN, 17)); // Increased event font size
            eventLog.setMargin(new Insets(10, 10, 10, 10));

            tabbedPane.addTab("  Eventos de Mercado  ", new JScrollPane(eventLog));

            // Reset event badge on tab focus
            tabbedPane.addChangeListener(e -> {
                if (tabbedPane.getSelectedIndex() == 1) {
                    unreadEvents = 0;
                    tabbedPane.setTitleAt(1, "  Eventos de Mercado  ");
                }
            });

            mainPanel.add(topPanel, BorderLayout.NORTH);
            mainPanel.add(tabbedPane, BorderLayout.CENTER);

            frame.add(mainPanel);
            frame.setVisible(true);
        });
    }

    public void updateQuote(String symbol, Double price, Double change) {
        if (!isGuiInitialized()) return;

        SwingUtilities.invokeLater(() -> {
            for (int i = 0; i < tableModel.getRowCount(); i++) {
                if (tableModel.getValueAt(i, 0).equals(symbol)) {
                    tableModel.removeRow(i);
                    break;
                }
            }

            String formattedPrice = priceFormat.format(price);
            String formattedChange = (change > 0 ? "+" : "") + percentFormat.format(change);

            tableModel.addRow(new Object[]{symbol, formattedPrice, formattedChange});
            symbolCount.setText("ATIVOS: " + tableModel.getRowCount());
        });
    }

    public void logEvent(String eventMessage) {
        if (!isGuiInitialized()) return;

        SwingUtilities.invokeLater(() -> {
            totalEvents++;
            String timestamp = LocalTime.now().format(timeFormatter);
            eventLog.insert("[" + timestamp + "] " + eventMessage + "\n", 0);
            eventLog.setCaretPosition(0);

            eventCount.setText("EVENTOS: " + totalEvents);

            if (tabbedPane.getSelectedIndex() != 1) {
                unreadEvents++;
                tabbedPane.setTitleAt(1, "  Eventos de Mercado  🔴 " + unreadEvents);
            }
        });
    }

    public void setMarketStatus(boolean connected) {
        if (!isGuiInitialized()) return;

        SwingUtilities.invokeLater(() -> {
            marketStatus.setText(connected ? "● MERCADO: AO VIVO" : "● MERCADO: OFFLINE");
            marketStatus.setForeground(connected ? new Color(46, 204, 113) : new Color(231, 76, 60));
        });
    }
}