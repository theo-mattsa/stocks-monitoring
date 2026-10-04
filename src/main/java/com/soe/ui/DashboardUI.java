package com.soe.ui;

import com.formdev.flatlaf.FlatDarkLaf;
import com.soe.domain.Quote;
import com.soe.domain.events.MarketEvent;
import com.soe.domain.events.PriceSpikeEvent;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import java.awt.*;
import java.net.URL;
import java.text.NumberFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DashboardUI {
    private static DashboardUI instance;
    private JFrame frame;
    private DefaultTableModel tableModel;
    private JTextArea eventLog;
    private JLabel marketStatus, symbolCount, eventCount;
    private JTabbedPane tabbedPane;
    private int totalEvents = 0, unreadEvents = 0;
    private final Map<String, ImageIcon> iconCache = new HashMap<>();

    private final NumberFormat priceFormat = NumberFormat.getCurrencyInstance(new Locale("pt", "BR"));
    private final NumberFormat percentFormat = NumberFormat.getPercentInstance(new Locale("pt", "BR"));
    private final NumberFormat volumeFormat = NumberFormat.getIntegerInstance(new Locale("pt", "BR"));
    private final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private DashboardUI() {
        priceFormat.setMinimumFractionDigits(2);
        priceFormat.setMaximumFractionDigits(2);
        percentFormat.setMinimumFractionDigits(2);
        percentFormat.setMaximumFractionDigits(2);
    }

    public static DashboardUI getInstance() {
        if (instance == null)
            instance = new DashboardUI();
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
            JLabel title = new JLabel("PAINEL DE MONITORAMENTO");
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

            // Tab 1: Live Quotes - Adicionada a coluna "VOLUME"
            String[] cols = { "LOGO", "ATIVO", "PREÇO", "VARIAÇÃO", "VOLUME" };
            tableModel = new DefaultTableModel(cols, 0) {
                @Override
                public boolean isCellEditable(int r, int c) {
                    return false;
                }

                @Override
                public Class<?> getColumnClass(int columnIndex) {
                    if (columnIndex == 0) {
                        return ImageIcon.class;
                    }
                    return super.getColumnClass(columnIndex);
                }
            };

            JTable quoteTable = new JTable(tableModel);
            quoteTable.setRowHeight(55);
            quoteTable.setFont(new Font("SansSerif", Font.PLAIN, 16));
            quoteTable.getTableHeader().setFont(new Font("SansSerif", Font.BOLD, 16));
            quoteTable.getColumnModel().getColumn(0).setMaxWidth(80);
            quoteTable.getColumnModel().getColumn(0).setPreferredWidth(80);

            DefaultTableCellRenderer centerRenderer = new DefaultTableCellRenderer();
            centerRenderer.setHorizontalAlignment(SwingConstants.CENTER);
            for (int i = 1; i < quoteTable.getColumnCount(); i++) {
                quoteTable.getColumnModel().getColumn(i).setCellRenderer(centerRenderer);
            }
            ((DefaultTableCellRenderer) quoteTable.getTableHeader().getDefaultRenderer())
                    .setHorizontalAlignment(SwingConstants.CENTER);

            quoteTable.getColumnModel().getColumn(0).setCellRenderer(new DefaultTableCellRenderer() {
                @Override
                public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected,
                        boolean hasFocus, int row, int column) {
                    JLabel label = (JLabel) super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row,
                            column);
                    label.setText("");
                    label.setHorizontalAlignment(SwingConstants.CENTER);
                    if (value instanceof ImageIcon icon) {
                        label.setIcon(icon);
                    } else {
                        label.setIcon(null);
                    }
                    return label;
                }
            });

            // Gain/loss cell rendering
            quoteTable.getColumnModel().getColumn(3).setCellRenderer(new DefaultTableCellRenderer() {
                @Override
                public Component getTableCellRendererComponent(JTable t, Object v, boolean s, boolean f, int r, int c) {
                    Component comp = super.getTableCellRendererComponent(t, v, s, f, r, c);
                    if (comp instanceof JLabel label) {
                        label.setHorizontalAlignment(SwingConstants.CENTER);
                    }
                    if (v != null) {
                        String val = v.toString();
                        comp.setForeground(val.startsWith("+") ? new Color(46, 204, 113)
                                : val.startsWith("-") ? new Color(231, 76, 60) : t.getForeground());
                    }
                    return comp;
                }
            });

            tabbedPane.addTab("  Cotações em Tempo Real  ", new JScrollPane(quoteTable));

            // Tab 2: Market Events Log
            eventLog = new JTextArea();
            eventLog.setEditable(false);
            eventLog.setFont(new Font("Monospaced", Font.PLAIN, 17));
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

    public void updateQuote(Quote quote) {
        if (!isGuiInitialized())
            return;
        fetchLogoAsync(quote.getSymbol(), quote.getLogourl());
        SwingUtilities.invokeLater(() -> {
            String formattedPrice = priceFormat.format(quote.getRegularMarketPrice());
            double changeVal = (quote.getRegularMarketChangePercent() != null) ? quote.getRegularMarketChangePercent()
                    : 0.0;
            String formattedChange = (changeVal > 0 ? "+" : "") + percentFormat.format(changeVal);
            
            Long volume = quote.getRegularMarketVolume();
            String formattedVolume = (volume != null) ? volumeFormat.format(volume) : "N/A";

            ImageIcon icon = iconCache.get(quote.getSymbol());
            int existingRow = -1;
            for (int i = 0; i < tableModel.getRowCount(); i++) {
                if (tableModel.getValueAt(i, 1).equals(quote.getSymbol())) {
                    existingRow = i;
                    break;
                }
            }
            if (existingRow != -1) {
                if (icon != null) {
                    tableModel.setValueAt(icon, existingRow, 0);
                }
                tableModel.setValueAt(formattedPrice, existingRow, 2);
                tableModel.setValueAt(formattedChange, existingRow, 3);
                tableModel.setValueAt(formattedVolume, existingRow, 4);
            } else {
                tableModel.addRow(new Object[] { icon, quote.getSymbol(), formattedPrice, formattedChange, formattedVolume });
            }
            symbolCount.setText("ATIVOS: " + tableModel.getRowCount());
        });
    }

    private void fetchLogoAsync(String symbol, String logoUrl) {
        if (logoUrl == null || logoUrl.isBlank() || iconCache.containsKey(symbol)) {
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                ImageIcon originalIcon = null;
                if (logoUrl.startsWith("/")) {
                    URL resourceUrl = getClass().getResource(logoUrl);
                    if (resourceUrl != null) {
                        originalIcon = new ImageIcon(resourceUrl);
                    }
                } else {
                    URL url = new URL(logoUrl);
                    originalIcon = new ImageIcon(url);
                }
                if (originalIcon != null && originalIcon.getIconWidth() > 0) {
                    Image scaledImage = originalIcon.getImage().getScaledInstance(42, 42, Image.SCALE_SMOOTH);
                    ImageIcon resizedIcon = new ImageIcon(scaledImage);
                    iconCache.put(symbol, resizedIcon);
                    SwingUtilities.invokeLater(() -> {
                        for (int i = 0; i < tableModel.getRowCount(); i++) {
                            if (tableModel.getValueAt(i, 1).equals(symbol)) {
                                tableModel.setValueAt(resizedIcon, i, 0);
                                break;
                            }
                        }
                    });
                }
            } catch (Exception e) {
                System.err.println("Error loading logo for " + symbol + ": " + e.getMessage());
            }
        });
    }

    public void logEvent(MarketEvent event) {
        if (!isGuiInitialized())
            return;

        SwingUtilities.invokeLater(() -> {
            totalEvents++;
            String timestamp = LocalTime.now().format(timeFormatter);
            String eventMessage = event.constructMessage();
            eventLog.insert("[" + timestamp + "] " + eventMessage + "\n", 0);
            eventLog.setCaretPosition(0);
            if (event instanceof PriceSpikeEvent) {
                Toolkit.getDefaultToolkit().beep();
            }
            eventCount.setText("EVENTOS: " + totalEvents);
            if (tabbedPane.getSelectedIndex() != 1) {
                unreadEvents++;
                tabbedPane.setTitleAt(1, "  Eventos de Mercado  🔴 " + unreadEvents);
            }
        });
    }

    public void setMarketStatus(boolean connected) {
        if (!isGuiInitialized())
            return;
        SwingUtilities.invokeLater(() -> {
            marketStatus.setText(connected ? "● MERCADO: AO VIVO" : "● MERCADO: OFFLINE");
            marketStatus.setForeground(connected ? new Color(46, 204, 113) : new Color(231, 76, 60));
        });
    }
}