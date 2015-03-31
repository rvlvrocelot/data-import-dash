class graph:

	colorarray = ['r','b','g','y','c','m']
	width = .35

	def __init__(self,x,y,title,ylable,legend=None,destination,N=9):
		self.x = x
		self.y = y
		self.title = title
		self.ylable = ylable
		self.Legend = Legend
		self.destination = destination
		self.N = N

		self.ind = np.arange(N)
		self.fig, self.ax = plt.subplots()
		self.ax.set_ylabel(ylable)
	    self.ax.set_title(title)
	    self.ax.set_xticks(ind+width)
	    self.ax.set_xticklabels( x )
	    self.plt.xticks(rotation=70)
	    self.plt.gcf().subplots_adjust(bottom=0.15)

	def generateStackedGraph(self):
		plt.savefig(destination)
		plt.clf()		


